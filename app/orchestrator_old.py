# app/orchestrator_old.py
import os
import uuid
import json
from app.s3_ingest import upload_file_with_metadata
from app.textract_processor import start_async_analysis_s3, get_async_analysis_result, extract_chunks_from_textract_response, detect_text_bytes
from app.pptx_parser import extract_chunks_from_pptx
from app.structured_adapter import excel_to_row_chunks
from app.bedrock_kb import BedrockKB
from app.kb_sync import sync_kb
from app.dynamo_client import DynamoClient
import boto3

S3_BUCKET = os.environ.get("S3_BUCKET")
CLAUDE_MODEL_ARN = os.environ.get("CLAUDE_MODEL_ARN")

class Orchestrator:
    def __init__(self):
        self.kb = BedrockKB()
        self.dyn = DynamoClient()

    def _generate_batch_id(self):
        return f"batch-{uuid.uuid4().hex[:8]}"

    def ingest_file_and_sync(self, use_case: str, kb_id: str, local_path: str, filename: str, uploader: str):
        """
        Upload file and create chunk JSONs (structured or unstructured) for fine-grained metadata,
        upload row/chunk JSONs to S3 so Bedrock can index them, then trigger KB sync (build).
        """
        batch_id = self._generate_batch_id()
        # upload original file
        up = upload_file_with_metadata(S3_BUCKET, use_case, batch_id, local_path, filename, uploader)
        file_id = uuid.uuid4().hex
        self.dyn.put_file(use_case, file_id, up["s3_uri"], up["sha256"], up["batch_id"], {"uploaded_by": uploader, "filename": filename})

        s3_uri = up["s3_uri"]
        # produce chunk JSONs depending on file type
        lower = filename.lower()
        chunk_objs = []
        # For PDF/large: use Textract async if object in S3
        if lower.endswith(".pdf") or lower.endswith(".png") or lower.endswith(".jpg") or lower.endswith(".jpeg"):
            # use async Textract on s3 object
            s3_key = up["key"]
            job_id = start_async_analysis_s3(S3_BUCKET, s3_key)
            tex_resp = get_async_analysis_result(job_id)
            chunks = extract_chunks_from_textract_response(tex_resp, s3_uri)
            chunk_objs.extend(chunks)
        elif lower.endswith(".pptx"):
            chunks = extract_chunks_from_pptx(local_path, s3_uri)
            chunk_objs.extend(chunks)
        elif lower.endswith(".xls") or lower.endswith(".xlsx") or lower.endswith(".csv"):
            uploaded_rows = excel_to_row_chunks(local_path, S3_BUCKET, use_case, batch_id, upload_rows=True)
            # excel_to_row_chunks already uploaded row-json files to s3 and returned objects
            chunk_objs.extend(uploaded_rows)
        else:
            # fallback: treat as text document
            with open(local_path, "rb") as f:
                b = f.read()
            tx = detect_text_bytes(b)
            # build chunk objects from detect_document_text (line-level)
            lines = []
            for block in tx.get("Blocks", []):
                if block.get("BlockType") == "LINE":
                    lines.append(block.get("Text"))
            for line in lines:
                chunk_objs.append({"chunk_id": uuid.uuid4().hex, "text": line, "metadata": {"doc_uri": s3_uri}})

        # Upload chunk JSONs to S3 under usecase/{usecase}/kb_chunks/{batch_id}/
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        for ch in chunk_objs:
            # ensure metadata exists
            if "metadata" not in ch:
                ch["metadata"] = {"doc_uri": s3_uri, "batch_id": batch_id}
            else:
                ch["metadata"]["batch_id"] = batch_id
            # upload chunk JSON so Bedrock can pick it up and index it with metadata
            key = f"usecase/{use_case}/kb_chunks/{batch_id}/{ch['chunk_id']}.json"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(ch).encode("utf-8"))
            # record chunk in DynamoDB as well for quick lookups
            self.dyn.put_chunk(use_case, ch["chunk_id"], ch)

        # trigger KB sync/build to ingest newly uploaded chunk JSONs and the original file
        sync_resp = sync_kb(kb_id)
        return {"status": "uploaded_and_sync_triggered", "batch_id": batch_id, "num_chunks": len(chunk_objs), "sync_resp": sync_resp}

    def query_kb_and_reconcile(self, use_case: str, kb_id: str, user_query: str, batch_id: str = None, global_template: str = None, usecase_template: str = None):
        """
        Build the final prompt, restrict retrieval to batch_id metadata if provided,
        call Bedrock retrieve_and_generate (Claude) and store the recon result with metadata and chunk references.
        """
        prompt = ""
        if global_template:
            prompt += global_template + "\n\n"
        if usecase_template:
            prompt += usecase_template + "\n\n"
        prompt += "User Query:\n" + user_query

        filters = None
        if batch_id:
            # Simple equals filter as example
            filters = {"equals": {"key": "batch_id", "value": batch_id}}

        resp = self.kb.retrieve_and_generate(kb_id, prompt, model_arn=CLAUDE_MODEL_ARN, retrieval_filters=filters)
        # Build recon record including chosen chunk references returned by Bedrock.
        # The shape of resp depends on Bedrock; assume it returns 'retrievedItems' with metadata + 'generatedText'
        recon_id = uuid.uuid4().hex
        record = {
            "recon_id": recon_id,
            "use_case": use_case,
            "kb_id": kb_id,
            "batch_id": batch_id,
            "prompt": prompt,
            "llm_model": CLAUDE_MODEL_ARN,
            "bedrock_raw_response": resp,
        }
        # Extract chunk references for easier audit if present
        refs = []
        # many Bedrock responses include retrieval results under resp['retrievedItems'] (check your API)
        for item in resp.get("retrievedItems", []) if isinstance(resp, dict) else []:
            # item may contain 'documentMetadata' or 'metadata'
            metadata = item.get("documentMetadata") or item.get("metadata") or {}
            refs.append({"kb_chunk_id": item.get("documentId") or item.get("id"), "metadata": metadata})
        record["references"] = refs
        self.dyn.put_recon_result(use_case, recon_id, record)
        return {"recon_id": recon_id, "record": record}
