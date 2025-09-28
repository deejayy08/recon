# app/s3_ingest.py
import os
import boto3
import hashlib
import time
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
S3 = boto3.client("s3", region_name=REGION)
SSE = "aws:kms"
KMS_KEY_ID = os.environ.get("KMS_KEY_ID")


def compute_sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def upload_file_with_metadata(bucket: str, use_case: str, batch_id: str, file_path: str, file_name: str, uploader: str):
    """
    Uploads original file to S3 under usecase prefix with metadata including batch_id and sha256.
    Returns dict with s3_uri, key, sha256, batch_id.
    """
    key = f"usecase/{use_case}/incoming/{batch_id}/{int(time.time())}-{file_name}"
    extra_args = {"ServerSideEncryption": SSE}
    if KMS_KEY_ID:
        extra_args["SSEKMSKeyId"] = KMS_KEY_ID

    with open(file_path, "rb") as f:
        content = f.read()

    sha256 = compute_sha256_bytes(content)
    metadata = {"sha256": sha256, "uploaded_by": uploader, "batch_id": batch_id}
    try:
        S3.put_object(Bucket=bucket, Key=key, Body=content, Metadata=metadata, **extra_args)
    except ClientError as e:
        raise

    return {"s3_uri": f"s3://{bucket}/{key}", "key": key, "sha256": sha256, "batch_id": batch_id}
