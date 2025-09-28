# app/structured_adapter.py
import pandas as pd
import json
import uuid
from app.s3_ingest import S3
import os

# boto3 S3 client for uploading small JSON rows (reuse same client)
import boto3
REGION = os.environ.get("AWS_REGION", "us-east-1")
S3 = boto3.client("s3", region_name=REGION)


def excel_to_row_chunks(path: str, s3_bucket: str, use_case: str, batch_id: str, upload_rows: bool = True):
    """
    Convert each row/cell into JSON chunk objects with metadata and optionally upload each JSON to S3
    under usecase/{use_case}/structured_rows/{batch_id}/
    Returns list of dicts: {chunk_id, text, metadata, s3_uri (optional)}
    """
    wb = pd.read_excel(path, sheet_name=None)
    uploaded = []
    for sheet_name, df in wb.items():
        for idx, row in df.iterrows():
            # row-level text (serialized)
            row_text = json.dumps(row.dropna().to_dict(), default=str)
            chunk_id = uuid.uuid4().hex
            metadata = {
                "doc_uri": f"s3://{s3_bucket}/usecase/{use_case}/incoming/{batch_id}/",  # points to source folder
                "sheet": sheet_name,
                "row": int(idx)
            }
            chunk = {"chunk_id": chunk_id, "text": row_text, "metadata": metadata}
            if upload_rows:
                key = f"usecase/{use_case}/structured_rows/{batch_id}/{chunk_id}.json"
                S3.put_object(Bucket=s3_bucket, Key=key, Body=json.dumps(chunk).encode("utf-8"))
                chunk["s3_uri"] = f"s3://{s3_bucket}/{key}"
            uploaded.append(chunk)
    return uploaded
