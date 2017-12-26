import boto3
import io
import pandas as pd

### Functions for reading and uploading data to/from S3

s3_bucket = "wri-public-data"
ACCESS_KEY = os.environ.get('aws_access_key_id')
SECRET_KEY = os.environ.get('aws_secret_access_key')

s3_bucket = "wri-public-data"
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
    )

s3_resource = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
    )

def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, key, bucket=s3_bucket):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
