import boto3
import io
import pandas as pd
import numpy as np
from dateutil import parser

s3_bucket = "wri-public-data"
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Functions for reading and uploading data to/from S3
def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, key, bucket=s3_bucket):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

# Provide function to map from wb_name to ISO3
# Load conversions from wb_name to iso3
CONVERSIONS = "resourcewatch/blog_data/GHG-GDP_Divergence_D3/Conversions/"
wb_name_to_iso3_conversion = read_from_S3(s3_bucket, CONVERSIONS+"World Bank to ISO3 name conversion.csv")
def add_iso(name):
    try:
        return(wb_name_to_iso3_conversion.loc[name,"ISO"])
    except:
        return(np.nan)

def fix_datetime_UTC(data_df, 
                     year_col=None, month_col=None, day_col=None, 
                     hour_col=None, min_col=None, sec_col = None,
                     date_col=None, time_col=None, 
                     datetime_col=None, 
                     date_pattern="%Y-%m-%dT%H:%M:%SZ"):
    """
    Desired datetime format: 2017-12-08T15:16:03Z
    Corresponding date_pattern for strftime: %Y-%m-%dT%H:%M:%SZ
    
    Depends on:
    from dateutil import parser
    """
    default_date = parser.parse("January 1 1900 00:00:00")
        
    
    
    # Need to provide the default parameter to parser.parse so that missing entries don't default to current date
    date_col = date_expression.apply(lambda date: parser.parse(date, default=default_date).strftime(date_pattern))
    
    return(date_col)



