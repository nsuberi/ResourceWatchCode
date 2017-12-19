import pandas as pd
import os
import logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import sys
import urllib
from collections import OrderedDict
import src.carto

import numpy as np
import boto3
import io
import requests as req

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

s3_bucket = "wri-public-data"

WB_DATA = "resourcewatch/world_bank_data_long_and_wide/"
CONVERSIONS = "resourcewatch/blog_data/GHG-GDP_Divergence_D3/Conversions/"

# Functions for reading and uploading data to/from S3
def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, bucket, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

# Provide function to map from wb_name to ISO3
# Load conversions from wb_name to iso3
wb_name_to_iso3_conversion = read_from_S3(s3_bucket, CONVERSIONS+"World Bank to ISO3 name conversion.csv")
def add_iso(name):
    try:
        return(wb_name_to_iso3_conversion.loc[name,"ISO"])
    except:
        return(np.nan)

def fetch_wb_data(codes_and_names):
    indicators = list(codes_and_names.keys())
    for indicator in indicators:
        # Results are paginated
        print(indicator)
        res = req.get("http://api.worldbank.org/countries/all/indicators/{}?format=json&per_page=10000".format(indicator))
        #print(res.text)
        data = pd.io.json.json_normalize(res.json()[1])
        data = data[["country.value", "date", "value"]]
        value_name = data_names_and_codes[indicator]
        data.columns = ["Country Name", "Year", value_name]
        data = data.set_index(["Country Name", "Year"])
        if all_world_bank_data:
            all_world_bank_data = all_world_bank_data.join(data, how="outer")
        else:
            all_world_bank_data = data

    all_world_bank_data = all_world_bank_data.reset_index()
    # Add ISO3 column
    all_world_bank_data["ISO3"] = list(map(add_iso, all_world_bank_data["Country Name"]))
    # Drop rows which don't have an ISO3 assigned
    # achieves the drop of "World" and "Europe" and other unwanted entries
    all_world_bank_data = all_world_bank_data.loc[pd.notnull(all_world_bank_data["ISO3"])]
    all_world_bank_data = all_world_bank_data.set_index(["Country Name", "Year"])

## Will need to make a better name
CARTO_SCHEMA = OrderedDict([
    ('ISO3', 'text'),
    ('Country', 'text'),
    ('Year', 'timestamp'),
    ('Value', 'numeric')
])

def main():

    ## World Bank data series codes and names
    data_codes_and_names = {'EG.ELC.ACCS.ZS': 'Access to electricity (% of population)',
     'EG.FEC.RNEW.ZS': 'Renewable energy consumption (% of total final energy consumption)',
     'IT.NET.USER.ZS': 'Individuals using the Internet (% of population)',
     'NE.CON.PRVT.PC.KD': 'Household final consumption expenditure per capita (constant 2010 US$)',
     'NV.IND.TOTL.KD': 'Industry, value added (constant 2010 US$)',
     'NY.GDP.TOTL.RT.ZS': 'Total natural resources rents (% of GDP)',
     'SG.GEN.PARL.ZS': 'Proportion of seats held by women in national parliaments (%)',
     'SL.EMP.TOTL.SP.ZS': 'Employment to population ratio, 15+, total (%) (modeled ILO estimate)',
     'SM.POP.NETM': 'Net migration',
     'SP.DYN.LE00.IN': 'Life expectancy at birth, total (years)',
     'SP.URB.TOTL.IN.ZS': 'Urban population (% of total)',
     'TM.VAL.MRCH.CD.WT': 'Merchandise imports (current US$)',
     'NY.GDP.MKTP.CD': 'GDP (current US$)'}

    all_world_bank_data = fetch_wb_data(data_codes_and_names)

    # Write to S3 and Carto the individual data sets
    for code, name in data_codes_and_names.items():
        long_form = all_world_bank_data[name]
        long_form = long_form.reset_index()
        long_form = long_form[pd.notnull(long_form[name])]

        # Write to S3
        write_to_S3(long_form, s3_bucket, WB_DATA + "wb_data_long_{}.csv".format(name.replace(" ", "_")))

        # Write to Carto
        CARTO_TABLE = code

        ### 1. Check if table exists and create table, if it does, drop and replace
        #dest_ids = []
        if not carto.tableExists(CARTO_TABLE):
            logging.info('Table {} does not exist'.format(CARTO_TABLE))
            carto.createTable(CARTO_TABLE, CARTO_SCHEMA)
        else:
            carto.dropTable(CARTO_TABLE)
            carto.createTable(CARTO_TABLE, CARTO_SCHEMA)

        ### 2. Fetch existing data - don't necessarily need to do this b/c data is small
        ### But could - and use the dedupe procedure to only update the necessary rows

        ### 3. Fetch data from source
        ### Can adapt the above code into a WB connector function, call it here,
        ### And encapsulate this code in a main() function
        #for dest, url in SOURCE_URLS.items():
        #    urllib.request.urlretrieve(url, os.path.join(DATA_DIR, dest))

        ### 4. Parse fetched data and generate unique ids
        ### This can be combined with step 2 above
        # rows = parseFloods(os.path.join(DATA_DIR, TABFILE), ENCODING, CARTO_SCHEMA.keys(), dest_ids)

        ### 5. Insert new observations
        rows = array(long_form)
        if len(rows):
            carto.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, rows)

        ### 6. Remove old observations
        ### Probably will not need this, as tables will not get too big

        #logging.info('Row count: {}, New: {}, Max: {}'.format(len(dest_ids), len(rows), MAXROWS))
        #if len(dest_ids) + len(rows) > MAXROWS and MAXROWS > len(rows):
        #    drop_ids = dest_ids[(MAXROWS - len(rows)):]
        #    carto.deleteRowsByIDs(CARTO_TABLE, "_UID", drop_ids)
