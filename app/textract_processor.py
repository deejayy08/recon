# app/textract_processor.py
"""
Textract-based extraction to get pages, tables, and cell-level references and produce chunk objects with metadata.

This uses synchronous calls for simple docs; for large PDFs you'd switch to start_document_analysis + polling.
"""
import boto3
import time
import uuid
from typing import List, Dict

textract = boto3.client("textract")


def detect_text_bytes(b: bytes) -> Dict:
    # For single images or very small PDFs: detect_document_text
    return textract.detect_document_text(Document={"Bytes": b})


def start_async_analysis_s3(bucket: str, key: str, feature_types=["TABLES", "FORMS"]):
    """
    For larger PDFs, start async analysis using S3 object reference.
    Returns JobId.
    """
    resp = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=feature_types
    )
    return resp["JobId"]


def get_async_analysis_result(job_id: str, poll_interval=5):
    while True:
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp.get("JobStatus")
        if status in ("SUCCEEDED", "FAILED"):
            return resp
        time.sleep(poll_interval)


def extract_chunks_from_textract_response(textract_resp: Dict, s3_uri: str) -> List[Dict]:
    """
    Parse Textract blocks and build chunks with metadata for page, table, row, column.
    For tables, Textract provides Table blocks; we reconstruct row/col relationships.
    Returns a list of chunk dicts: {chunk_id, text, metadata}
    """
    blocks = textract_resp.get("Blocks", [])
    # Index blocks by id
    by_id = {b["Id"]: b for b in blocks}

    # Build pages mapping
    pages = {}
    for b in blocks:
        if b["BlockType"] == "PAGE":
            pages[b["Id"]] = b

    chunks = []

    # Extract LINE blocks for plain text, attach page number if possible
    for b in blocks:
        if b["BlockType"] == "LINE":
            page = None
            if "Page" in b:
                page = b.get("Page")
            text = b.get("Text", "")
            chunk = {
                "chunk_id": uuid.uuid4().hex,
                "text": text,
                "metadata": {"doc_uri": s3_uri, "page": page}
            }
            chunks.append(chunk)

    # Extract TABLES -> reconstruct rows and cells
    # Textract provides TABLE blocks with Relationships -> CHILD referencing CELL blocks.
    table_count = 0
    for b in blocks:
        if b["BlockType"] == "TABLE":
            table_count += 1
            table_id = table_count
            # gather cell blocks
            cell_blocks = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cid in rel.get("Ids", []):
                        cb = by_id.get(cid)
                        if cb and cb["BlockType"] == "CELL":
                            cell_blocks.append(cb)
            # Build rows based on RowIndex
            rows = {}
            for cell in cell_blocks:
                row_index = cell.get("RowIndex", 0)
                col_index = cell.get("ColumnIndex", 0)
                # get text inside this cell by following its child relationships
                cell_text = ""
                for rel in cell.get("Relationships", []):
                    if rel["Type"] == "CHILD":
                        for child_id in rel.get("Ids", []):
                            child = by_id.get(child_id)
                            if child and child.get("BlockType") in ("WORD", "LINE"):
                                cell_text += (child.get("Text", "") + " ")
                rows.setdefault(row_index, {})[col_index] = cell_text.strip()
            # create chunk per row (row-level chunk) and cell-level chunks
            for r_idx, cols in rows.items():
                row_text = " | ".join([cols[c] for c in sorted(cols.keys())])
                chunk_row = {
                    "chunk_id": uuid.uuid4().hex,
                    "text": row_text,
                    "metadata": {
                        "doc_uri": s3_uri,
                        "page": b.get("Page", None),
                        "table": table_id,
                        "row": r_idx
                    }
                }
                chunks.append(chunk_row)
                for c_idx, c_text in cols.items():
                    chunk_cell = {
                        "chunk_id": uuid.uuid4().hex,
                        "text": c_text,
                        "metadata": {
                            "doc_uri": s3_uri,
                            "page": b.get("Page", None),
                            "table": table_id,
                            "row": r_idx,
                            "col": c_idx
                        }
                    }
                    chunks.append(chunk_cell)

    return chunks
