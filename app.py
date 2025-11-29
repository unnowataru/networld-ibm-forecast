import os
import ibm_boto3
from ibm_botocore.client import Config

COS_ENDPOINT = os.environ["COS_ENDPOINT"]
COS_APIKEY = os.environ["COS_APIKEY"]
COS_INSTANCE_CRN = os.environ["COS_INSTANCE_CRN"]
COS_BUCKET = os.environ["COS_BUCKET"]

cos = ibm_boto3.resource(
    "s3",
    ibm_api_key_id=COS_APIKEY,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT,
)

# 疎通テスト: inputs/見積データ.csv → outputs/ping.csv
src_key = "inputs/見積データ.csv"
dst_key = "outputs/ping.csv"

body = cos.Object(COS_BUCKET, src_key).get()["Body"].read()
cos.Object(COS_BUCKET, dst_key).put(Body=body)
print("OK:", dst_key)
