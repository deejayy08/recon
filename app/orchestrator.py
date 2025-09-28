# app/orchestrator.py
import os
import uuid
import json
import boto3
import time
from app.s3_ingest import upload_file_with_metadata
from app.textract_processor import start_async_analysis_s3, get_async_analysis_result, extract_chunks_from_textract_response, detect_text_bytes
from app.pptx_parser import extract_chunks_from_pptx
from app.structured_adapter import excel_to_row_chunks
from app.bedrock_kb import BedrockKB
from app.kb_sync import sync_kb, get_sync_status
from app.dynamo_client import DynamoClient

S3_BUCKET = os.environ.get("S3_BUCKET")
CLAUDE_MODEL_ARN = os.environ.get("CLAUDE_MODEL_ARN")


class Orchestrator:
    def __init__(self):
        self.kb = BedrockKB()
        self.dyn = DynamoClient()

    def _generate_batch_id(self):
        return f"batch-{uuid.uuid4().hex[:8]}"

    def ingest_file_and_sync(self, use_case: str, kb_id: str, local_path: str, filename: str, uploader: str, wait_build=True, poll_interval=15, timeout=600):
        """
        Upload file, create chunks, upload to S3 for KB, trigger KB sync, and optionally poll until build completes.
        """
        batch_id = self._generate_batch_id()
        file_and_meta = upload_file_with_metadata(S3_BUCKET, use_case, batch_id, local_path, filename, uploader)
        file_id = uuid.uuid4().hex
        self.dyn.put_file(use_case, file_id, file_and_meta["s3_uri"], file_and_meta["sha256"], file_and_meta["batch_id"], {"uploaded_by": uploader, "filename": filename})

        s3_uri = file_and_meta["s3_uri"]
        lower = filename.lower()
        chunk_objs = []

        if lower.endswith(".pdf") or lower.endswith(".png") or lower.endswith(".jpg") or lower.endswith(".jpeg"):
            job_id = start_async_analysis_s3(S3_BUCKET, file_and_meta["key"])
            tex_resp = get_async_analysis_result(job_id)
            chunks = extract_chunks_from_textract_response(tex_resp, s3_uri)
            chunk_objs.extend(chunks)
        elif lower.endswith(".pptx"):
            chunks = extract_chunks_from_pptx(local_path, s3_uri)
            chunk_objs.extend(chunks)
        elif lower.endswith(".xls") or lower.endswith(".xlsx") or lower.endswith(".csv"):
            uploaded_rows = excel_to_row_chunks(local_path, S3_BUCKET, use_case, batch_id, upload_rows=True)
            chunk_objs.extend(uploaded_rows)
        else:
            with open(local_path, "rb") as f:
                b = f.read()
            tx = detect_text_bytes(b)
            for block in tx.get("Blocks", []):
                if block.get("BlockType") == "LINE":
                    chunk_objs.append({"chunk_id": uuid.uuid4().hex, "text": block.get("Text"), "metadata": {"doc_uri": s3_uri}})

        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        for ch in chunk_objs:
            ch.setdefault("metadata", {})["batch_id"] = batch_id
            key = f"usecase/{use_case}/kb_chunks/{batch_id}/{ch['chunk_id']}.json"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(ch).encode("utf-8"))
            self.dyn.put_chunk(use_case, ch["chunk_id"], ch)

        sync_resp = sync_kb(kb_id)

        if wait_build:
            start = time.time()
            while True:
                status_resp = get_sync_status(kb_id)
                status = status_resp.get("status") or status_resp.get("buildStatus")
                if status in ("COMPLETE", "SUCCEEDED"):
                    break
                if status in ("FAILED", "ERROR"):
                    raise RuntimeError(f"KB build failed: {status_resp}")
                if time.time() - start > timeout:
                    raise TimeoutError("Timed out waiting for KB build")
                time.sleep(poll_interval)

        return {"status": "uploaded_and_indexed", "batch_id": batch_id, "num_chunks": len(chunk_objs)}

    def query_kb_and_reconcile(self, use_case: str, kb_id: str, user_query: str, batch_id: str = None, global_template: str = None, usecase_template: str = None):
        prompt = ""
        if global_template:
            prompt += global_template + "\n\n"
        if usecase_template:
            prompt += usecase_template + "\n\n"
        prompt += "User Query:\n" + user_query

        filters = None
        if batch_id:
            filters = {"equals": {"key": "batch_id", "value": batch_id}}

        resp = self.kb.retrieve_and_generate(kb_id, prompt, model_arn=CLAUDE_MODEL_ARN, retrieval_filters=filters)
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
        refs = []
        for item in resp.get("retrievedItems", []) if isinstance(resp, dict) else []:
            metadata = item.get("documentMetadata") or item.get("metadata") or {}
            refs.append({"kb_chunk_id": item.get("documentId") or item.get("id"), "metadata": metadata})
        record["references"] = refs
        self.dyn.put_recon_result(use_case, recon_id, record)
        return {"recon_id": recon_id, "record": record}

    def list_recons(self, use_case: str, limit=10):
        """
        Return latest N recon results for replay mode.
        """
        table = self.dyn.table_recon
        resp = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("use_case").eq(use_case),
            Limit=limit,
            ScanIndexForward=False
        )
        return resp.get("Items", [])

    def fetch_reference_snippet(self, ref: dict):
        """
        Given a reference metadata, fetch relevant snippet from S3.
        For Excel rows: download and show row values.
        For PDF page/table references: return metadata stub.
        """
        meta = ref.get("metadata", {})
        s3_uri = meta.get("doc_uri")
        if not s3_uri:
            return "No doc_uri in metadata"

        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        if key.endswith(".xlsx") or key.endswith(".xls"):
            import pandas as pd
            import io
            df = pd.read_excel(io.BytesIO(obj["Body"].read()), sheet_name=None)
            sheet = meta.get("sheet")
            row = meta.get("row")
            if sheet in df and row is not None:
                if row in df[sheet].index:
                    return df[sheet].loc[row].to_dict()
        elif key.endswith(".csv"):
            import pandas as pd
            import io
            df = pd.read_csv(io.BytesIO(obj["Body"].read()))
            row = meta.get("row")
            if row is not None and row < len(df):
                return df.iloc[row].to_dict()
        else:
            # For unstructured, return metadata (page, table, row, col)
            return f"Reference snippet metadata: {meta}"
        return "Snippet not found"
