# app/bedrock_kb.py
import os
import uuid
import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
BEDROCK_AGENT = boto3.client("bedrock-agent", region_name=REGION)
ROLE_ARN = os.environ.get("BEDROCK_ROLE_ARN")
EMBEDDING_MODEL_ARN = os.environ.get("EMBEDDING_MODEL_ARN")


class BedrockKB:
    def __init__(self, agent_client=None):
        self.client = agent_client or BEDROCK_AGENT

    def create_kb(self, name: str, s3_bucket: str, s3_prefix: str, structured_config: dict = None, description: str = None):
        req = {
            "name": f"{name}-{uuid.uuid4().hex[:6]}",
            "description": description or f"KB for {name}",
            "roleArn": ROLE_ARN,
            "knowledgeBaseConfiguration": {
                "type": "VECTOR",
                "vectorKnowledgeBaseConfiguration": {"embeddingModelArn": EMBEDDING_MODEL_ARN}
            },
            "dataSources": [
                {
                    "type": "S3",
                    "s3Configuration": {"bucketName": s3_bucket, "path": s3_prefix}
                }
            ]
        }
        if structured_config:
            req["dataSources"][0]["s3Configuration"]["structuredDataConfig"] = structured_config
        resp = self.client.create_knowledge_base(**req)
        return resp

    def start_build(self, kb_id: str):
        try:
            resp = self.client.start_knowledge_base_build(knowledgeBaseId=kb_id)
            return resp
        except ClientError as e:
            return {"error": str(e)}

    def get_build_status(self, kb_id: str):
        try:
            resp = self.client.get_knowledge_base_build(knowledgeBaseId=kb_id)
            return resp
        except ClientError as e:
            return {"error": str(e)}

    def retrieve_and_generate(self, kb_id: str, prompt: str, model_arn: str = None, max_output_tokens: int = 1024, retrieval_filters: dict = None):
        """
        Calls Bedrock's retrieve_and_generate on the KB.
        retrieval_filters is passed in as {"filters": { ... }} depending on API.
        Common filter: restrict to batch_id metadata.
        """
        params = {
            "knowledgeBaseId": kb_id,
            "input": {"text": prompt},
            "maxOutputTokens": max_output_tokens
        }
        if model_arn:
            params["modelArn"] = model_arn
        if retrieval_filters:
            params["retrievalConfiguration"] = {"filters": retrieval_filters}
        resp = self.client.retrieve_and_generate(**params)
        return resp
