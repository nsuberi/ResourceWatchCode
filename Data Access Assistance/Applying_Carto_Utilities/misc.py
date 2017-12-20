import boto3
import io
import pandas as pd
import numpy as np

from dateutil import parser
import pytz
import datetime


### Functions for reading and uploading data to/from S3

s3_bucket = "wri-public-data"
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, key, bucket=s3_bucket):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

    
    
    
### Standardizing ISO3 codes

CONVERSIONS = "resourcewatch/blog_data/GHG-GDP_Divergence_D3/Conversions/"
wb_name_to_iso3_conversion = read_from_S3(s3_bucket, CONVERSIONS+"World Bank to ISO3 name conversion.csv")
def add_iso(name):
    try:
        return(wb_name_to_iso3_conversion.loc[name,"ISO"])
    except:
        return(np.nan)

# Dropping any countries not desired in the result

drop_patterns = "Arab World, Middle income, Europe & Central Asia (IDA & IBRD countries), IDA total, Latin America & the Caribbean (IDA & IBRD countries), Middle East & North Africa (IDA & IBRD countries), blank (ID 268), Europe & Central Asia (excluding high income), IBRD only, IDA only, Early-demographic dividend, Latin America & the Caribbean (excluding high income), Middle East & North Africa, Middle East & North Africa (excluding high income), Late-demographic dividend, Pacific island small states, Europe & Central Asia, European Union, High income, IDA & IBRD total, IDA blend, Caribbean small states, Central Europe and the Baltics, East Asia & Pacific, East Asia & Pacific (excluding high income), Low & middle income, Lower middle income, Other small states, Latin America & Caribbean, East Asia & Pacific (IDA & IBRD countries), Euro area, OECD members, North America, Middle East & North Africa (excluding high income), Post-demographic dividend, Small states, South Asia, Upper middle income, World, heavily indebted poor countries (HIPC), Least developed countries: UN classification, blank (ID 267), blank (ID 265), Latin America & Caribbean, IDA & IBRD total, IBRD only, Europe & Central Asia, sub-Saharan Africa (excluding high income), Macao SAR China, sub-Saharan Africa, pre-demographic dividend, South Asia (IDA & IBRD), sub-Saharan Africa (IDA & IBRD), Upper middle income, fragile and conflict affected"
drop_patterns = [patt.strip() for patt in drop_patterns.split(",")]

def pick_wanted_entities(entities, drop_patterns=drop_patterns):
    """
    Input: 
    * a list of entities that correspond to a dataframe of observations for which these may be in the index
    * a list of which entities you'd like to eliminate
    
    Output: which indices to keep from the originating dataframe to eliminate the desired entities
    """
    
    ix_to_keep = [ix for ix, entity in enumerate(entities) if entity not in drop_patterns]
    return(ix_to_keep)

entities = ["France", "Ghana", "Middle income", "Europe & Central Asia (IDA & IBRD countries)", "IDA total"]

logging.info(pick_wanted_entities(entities))
logging.info(pick_wanted_entities(entities, drop_patterns=["France", "Ghana"]))
    
### Standardizing datetimes
    
def structure_dttm_from_parts(row, dttm_elems, dttm_pattern):
    dt = datetime.datetime(year=int(row[dttm_elems["year_col"]]), 
                           month=int(row[dttm_elems["month_col"]]),
                           day=int(row[dttm_elems["day_col"]]))
    if "hour_col" in dttm_elems:
        dt = dt.replace(hour=int(row[dttm_elems["hour_col"]]))
    if "min_col" in dttm_elems:
        dt = dt.replace(minute=int(row[dttm_elems["min_col"]]))
    if "sec_col" in dttm_elems:
        dt = dt.replace(second=int(row[dttm_elems["sec_col"]]))
    if "milli_col" in dttm_elems:
        dt = dt.replace(milliseconds=int(row[dttm_elems["milli_col"]]))
    if "micro_col" in dttm_elems:
        dt = dt.replace(microseconds=int(row[dttm_elems["micro_col"]]))
    if "tzinfo_col" in dttm_elems:
        timezone = pytz.timezone(row[dttm_elems["tzinfo_col"]])
        dt = timezone.localize(dt)
    
    dttm_str = dt.strftime(dttm_pattern)
    return(dttm_str)

def fix_datetime_UTC(data_df, dttm_elems_in_sep_columns=True, 
                     dttm_elems={},
                     dttm_columnz=None, 
                     dttm_pattern="%Y-%m-%dT%H:%M:%SZ"):
    """
    Desired datetime format: 2017-12-08T15:16:03Z
    Corresponding date_pattern for strftime: %Y-%m-%dT%H:%M:%SZ
    
    If date_elems_in_sep_columns=True, then there will be a dictionary date_elems
    That at least contains the following elements:
    date_elems = {"year_col":`int or string`,"month_col":`int or string`,"day_col":`int or string`}
    OPTIONAL KEYS IN date_elems:
    * hour_col
    * min_col
    * sec_col
    * milli_col
    * micro_col
    * tz_col
    
    Depends on:
    from dateutil import parser
    """
    default_date = parser.parse("January 1 1900 00:00:00")
        
    # Mutually exclusive to provide broken down datetime factors, 
    # and either a date, time, or datetime object
    if dttm_elems_in_sep_columns:
        assert(type(dttm_elems)==dict)
        assert(dttm_columnz==None)
        
        tmp = data_df.copy()
        if "year_col" not in dttm_elems:
            dttm_elems["year_col"] = "year_tmp"
        if dttm_elems["year_col"] not in tmp.columns:
            tmp[dttm_elems["year_col"]] = 1990
            
        if "month_col" not in dttm_elems:
            dttm_elems["month_col"] = "month_tmp"
        if dttm_elems["month_col"] not in tmp.columns:
            tmp[dttm_elems["month_col"]] = 1
            
        if "day_col" not in dttm_elems:
            dttm_elems["day_col"] = "day_tmp"
        if dttm_elems["day_col"] not in tmp.columns:
            tmp[dttm_elems["day_col"]] = 1
        
        dttm_col = tmp.apply(lambda row: structure_dttm_from_parts(row, dttm_elems, dttm_pattern), axis=1)
        
    else:
        # Make sure it is possible to treat dttm_columnz as a list
        assert(dttm_columnz!=None)
        if type(dttm_columnz) != list:
            assert(type(dttm_columns) in [str, int, float])
            dttm_columnz = list(dttm_columnz)
            
        # No matter what, this runs over a Series, and thus you don't have to set axis=1
        if len(dttm_columnz)>1:
            # Need to provide the default parameter to parser.parse so that missing entries don't default to current date
            dttm_col = data_df[dttm_columns].apply(lambda row: parser.parse(row[dttm_col], default=default_date).strftime(dttm_pattern))
        else:
            # pack together then send through apply
            dttm_contents = data_df[dttm_columnz[0]]
            for col in dttm_columns[1:]:
                dttm_contents = dttm_contents + " " + data_df[col]
            dttm_col = dttm_contents.apply(lambda dttm: parser.parse(dttm, default=default_date).strftime(dttm_pattern))
    
    return(dttm_col)


