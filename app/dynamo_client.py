# app/dynamo_client.py
import os
import boto3
import time
import uuid
from boto3.dynamodb.conditions import Key

REGION = os.environ.get("AWS_REGION", "us-east-1")
dynamodb = boto3.resource("dynamodb", region_name=REGION)

TABLE_FILES = os.environ.get("DYNAMODB_TABLE_FILES")
TABLE_CHUNKS = os.environ.get("DYNAMODB_TABLE_CHUNKS")
TABLE_RECON = os.environ.get("DYNAMODB_TABLE_RECON")


class DynamoClient:
    def __init__(self):
        self.table_files = dynamodb.Table(TABLE_FILES)
        self.table_chunks = dynamodb.Table(TABLE_CHUNKS)
        self.table_recon = dynamodb.Table(TABLE_RECON)

    def put_file(self, use_case: str, file_id: str, s3_uri: str, sha256: str, batch_id: str, meta: dict):
        item = {
            "use_case": use_case,
            "file_id": file_id,
            "s3_uri": s3_uri,
            "sha256": sha256,
            "batch_id": batch_id,
            "meta": meta,
            "uploaded_at": int(time.time())
        }
        self.table_files.put_item(Item=item)
        return item

    def put_chunk(self, use_case: str, chunk_id: str, chunk_obj: dict):
        item = {"use_case": use_case, "chunk_id": chunk_id, "chunk": chunk_obj}
        self.table_chunks.put_item(Item=item)
        return item

    def put_recon_result(self, use_case: str, recon_id: str, payload: dict):
        item = {"use_case": use_case, "recon_id": recon_id, "payload": payload, "created_at": int(time.time())}
        self.table_recon.put_item(Item=item)
        return item
