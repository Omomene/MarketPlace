from airflow.sdk.bases.hook import BaseHook
import boto3
import json
from datetime import datetime


class MinioClient:

    def __init__(self):

        conn = BaseHook.get_connection("minio_local")

        self.client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000", 
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name="us-east-1"
        )

        self.bucket = "data-lake"

    def upload_json(self, data, path_prefix):
        date = datetime.utcnow().strftime("%Y-%m-%d")

        key = f"{path_prefix}/dt={date}/data.json"

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data).encode("utf-8")
        )

        return key