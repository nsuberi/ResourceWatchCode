





####
## THIS NEEDS TO BE RUN ON A WEB SERVER,
## NOT PURELY INSIDE A DOCKER CONTAINER LIKE SKETCHED BELOW
####

import cartoframes
import pandas as pd
pd.options.display.max_columns = 200

import requests as req
import json
import boto3
import io
from datetime import datetime
from collections import defaultdict

import sys
import logging
import os
LOG_LEVEL = logging.INFO
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

DATA_DIR = 'data/'
CARTO_WRI_RW_USER = os.environ.get(CARTO_WRI_RW_USER, None)
CARTO_WRI_RW_KEY = os.environ.et(CARTO_WRI_RW_KEY, None)

####
## Set up S3 connections
####

def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, bucket, key):
    csv_buffer = io.StringIO()
    # Need to set encoding in Python2... default of 'ascii' fails
    df.to_csv(csv_buffer, encoding='utf-8')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

def s3_init():

    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')

    s3_bucket = "wri-public-data"
    s3_folder = "resourcewatch/wide_to_long/"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    return s3_client, s3_resource

####
## Getting metadata from RW API
####

def grab_api_metadata(provider):

    url = "https://api.resourcewatch.org/v1/dataset?sort=slug,-provider,userId&status=saved&includes=metadata,vocabulary,widget,layer"

    # page[size] tells the API the maximum number of results to send back
    # There are currently between 200 and 300 datasets on the RW API
    payload = { "application":"rw", "page[size]": 1000}

    # Request all datasets, and extract the data from the response
    res = req.get(url, params=payload)
    data = res.json()["data"]

    ### Convert the json object returned by the API into a pandas DataFrame
    # Another option: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.io.json.json_normalize.html
    datasets_on_api = {}
    for ix, dset in enumerate(data):
        atts = dset["attributes"]
        metadata = atts["metadata"]
        layers = atts["layer"]
        widgets = atts["widget"]
        tags = atts["vocabulary"]
        datasets_on_api[dset["id"]] = {
            "name":atts["name"],
            "table_name":atts["tableName"],
            "provider":atts["provider"],
            "date_updated":atts["updatedAt"],
            "num_metadata":len(metadata),
            "metadata": metadata,
            "num_layers":len(layers),
            "layers": layers,
            "num_widgets":len(widgets),
            "widgets": widgets,
            "num_tags":len(tags),
            "tags":tags
        }

    # Create the DataFrame, name the index, and sort by date_updated
    # More recently updated datasets at the top
    current_datasets_on_api = pd.DataFrame.from_dict(datasets_on_api, orient='index')
    current_datasets_on_api.index.rename("Dataset", inplace=True)
    current_datasets_on_api.sort_values(by=["date_updated"], inplace=True, ascending = False)

    # Select all Carto datasets on the API:
    provider_ids = (current_datasets_on_api["provider"]==provider)
    provider_metadata = current_datasets_on_api.loc[provider_ids]

    logging.info("Number of datasets for provider {}: {}".format(provider, provider_metadata.shape[0]))

    return provider_metadata

####
## Setting up config for georeferencing
####



def prepare_carto_data(datasets, carto_metadata):
    '''
    Inputs:
    Datasets, a pandas dataframe w/ cols `rw_id, api_id, country_name_col, country_iso_col`
    Provider_metadata, a pandas dataframe w/ metadata for each dataset for the given provider on the API
    '''

    tables = {}
    tables['country_aliases'] = {
        'data':cc.read('country_aliases_extended')
    }
    # As a list
    dset_list = [list(dset) for dset in datasets.values]
    # As a dict
    dset_dict = datasets.to_dict(orient='index')

    ###
    ## This is currently built out to read data already on the API...
    ## could be used to prepare data for upload to the API
    ###
    for rw_id, api_id in datasets.items():
        logging.info(rw_id)
        # Fetch table name
        try:
            table_name = carto_metadata.loc[api_id]['table_name']
        except:
            msg = '***** ' + rw_id + ' not in carto_data *****'
            table_info[table_name] = {
                'error':msg
            }
            logging.info(msg)

        # Fetch data
        try:
            tables[table_name]['data'] = cc.read(table_name)
            if 'config_options' not in table_info[table_name]:
                logging.info('updating table_info for ' + table_name)
                table_info[table_name] = {
                    'config_options':{}
                }
        except:
            msg = '***** Problem fetching ' + table_name + ' from Carto server *****'
            if table_name in table_info:
                if 'error' in table_info[table_name]:
                    logging.info(table_info[table_name]['error'])
            else:
                table_info[table_name] = {
                    'error':msg
                }
                logging.info(msg)


    for name, info in tables.items():
        logging.info('table name: ' + name)
        try:
            logging.info('table shape: ' + str(info['data'].shape))
        except:
            logging.info(info['error'])

    return tables







####
## WORKFLOW
####

def main():

    if GRAB_FROM_RW_API:
        rw_api_defs = os.environ.get('RW_API_DEFS', pd.DataFrame())
        datasets = pd.read_csv(DATA_DIR + rw_api_defs, header=1)
        cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(CARTO_WRI_RW_USER),
                                      api_key=CARTO_WRI_RW_KEY)

        carto_data = grab_api_metadata(provider="cartodb")



    if PROCESS_FROM_URL:
        ####
        ## TO DO: configure to allow this to run as a microservice and receive requests
        ####
        datasets = os.environ.get('URL_PASSED_INFO', None)
