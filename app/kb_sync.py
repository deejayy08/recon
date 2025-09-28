# app/kb_sync.py
import boto3
from botocore.exceptions import ClientError
import os

REGION = os.environ.get("AWS_REGION", "us-east-1")
BEDROCK_AGENT = boto3.client("bedrock-agent", region_name=REGION)


def sync_kb(kb_id: str):
    try:
        resp = BEDROCK_AGENT.start_knowledge_base_build(knowledgeBaseId=kb_id)
        return resp
    except ClientError as e:
        return {"error": str(e)}


def get_sync_status(kb_id: str):
    try:
        resp = BEDROCK_AGENT.get_knowledge_base_build(knowledgeBaseId=kb_id)
        return resp
    except ClientError as e:
        return {"error": str(e)}
