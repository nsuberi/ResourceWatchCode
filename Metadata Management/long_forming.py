import cartoframes
import pandas as pd
pd.options.display.max_columns = 200

import requests as req
import json
import boto3
import io
import datetime
from dateutil import parser

import sys
import logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)


ACCESS_ID=''
SECRET_KEY=''

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=SECRET_KEY
)
s3_resource = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=SECRET_KEY
)

s3_bucket = "wri-public-data"
s3_folder = "resourcewatch/wide_to_long/"

# Functions for reading and uploading data to/from S3
def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, bucket, key):
    csv_buffer = io.BytesIO()
    # Need to set encoding in Python2... default of 'ascii' fails
    df.to_csv(csv_buffer, encoding='utf-8')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())



# Set up carto context
user = 'wri-rw' # 'rw-nrt'
key = {
    'wri-rw':'',
    'rw-nrt':''
}
cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(user),
                              api_key=key[user])




# Access data from API
# Base URL for getting dataset metadata from RW API
# Metadata = Data that describes Data
url = "https://api.resourcewatch.org/v1/dataset?sort=slug,-provider,userId&status=saved&includes=metadata,vocabulary,widget,layer"

# page[size] tells the API the maximum number of results to send back
# There are currently between 200 and 300 datasets on the RW API
payload = { "application":"rw", "page[size]": 1000}

# Request all datasets, and extract the data from the response
res = req.get(url, params=payload)
data = res.json()["data"]

#############################################################

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




# Carto datasets
provider = "cartodb"
carto_ids = (current_datasets_on_api["provider"]==provider)
carto_data = current_datasets_on_api.loc[carto_ids]

logging.info("Number of Carto datasets: ", carto_data.shape[0])


# Long form table possibilities
long_forms = {"cit.022":"6b670396-c52c-430c-b5bb-20693da03b60","cit.025":"d38d0d5c-31b1-47f4-9d2e-d8fba4c7d083","cit.028":"35ce2b98-adbb-4873-b334-d7b1cc542de7","cit.029":"10337db6-8321-445e-a60b-28fc1e114f29","cli.007":"3d2ce960-abda-4c9c-bd29-1929e9ca24c9","cli.008":"3a46f6b4-0eec-49d4-bbfc-e2e8f64e6117","com.006":"2e31a1f3-576b-46b4-84f0-3f0cc399f887","com.007":"fe311144-8c0e-4440-b068-6efd057e0f6a","com.009":"c61c364b-1d68-4dd9-ae3d-76c2a0022280","com.010":"52c55378-0484-48c3-92fc-3ee94d21c716","com.015":"c18a38cd-94ff-48cd-818f-6ffb05992abb","com.019":"5e3a3a9f-7380-47c0-ad84-2c193861e106","com.028":"62c988a7-1e4d-418e-87bf-a743e24209e8","ene.012":"d446a52e-c4c1-4e74-ae30-3204620a0365","ene.021":"2c092793-aa3a-4520-959c-ad48165dcae4","ene.022":"d639909f-bcf3-4875-b8c3-35f030b68ed3","ene.028":"c665f519-eef9-4f67-a8bf-7e3e6dc8bfcd","foo.006":"2034a766-6e8a-416d-b8ab-9b7b3e3abb15","foo.015":"4338471d-881a-475f-8bd9-60c4d48b8e12","foo.019":"8bc79a36-d77e-4ee3-b9bc-c77146cfc503","foo.040":"91ff1359-6680-49bc-8002-20256e999993","foo.041":"ccfb322a-20aa-4132-b58b-0f76acec8f5a","foo.042":"7a551dd8-b59c-4f59-9d50-c92cb61c5799","foo.043":"95b013a3-389a-4367-83b7-c9d68c28c406","for.021":"05b7c688-09ba-4f33-90ea-185a1039df43","soc.004":"bea122ce-1e4b-465d-8b7b-fa11aadd20f7","cit.017":"0303127a-70b0-4164-9251-d8162615d058","soc.005":"a7067e9f-fe40-4338-85da-13a6071c76fe","soc.006":"a89c95c7-0b82-4162-b9d8-cc0205e9f7ec","soc.008":"00abb46f-34e2-4bf7-be30-1fb0b1de022f","soc.015":"e8f53f73-d77c-485a-a2a6-1c47ea4aead9","soc.020":"f8d3e79c-c3d0-4f9a-9b68-9c5ad1f025e4","soc.023":"d3a6b89f-cf5c-40cf-b2b3-ac1c8315c648","soc.025":"11278cb6-b298-49a1-bf71-f1e269f40758","soc.029":"7793f46c-a48a-466f-a8ce-ca1a87b7aeed","soc.035":"e7780d53-ad80-45bd-a271-79615ee97a37","soc.036":"8671f536-1979-4b6f-a147-70152fcb44ed","soc.039":"b37048be-9b23-4458-a047-888956c69aa1","soc.040":"37d04efc-0ab2-4499-a891-54dca1013c74","soc.062":"5e69cfac-1f68-4864-a19a-3c1bdb180100","wat.005":"1b97e47e-ca18-4e50-9aae-a2853acca3f0","cli.029":"fa6443ff-eb95-4d0f-84d2-f0c91682efdf","ene.017":"75061411-3afd-442f-b7d6-7607f97f639b"}
long_forms




# Load config options
try:
    table_info = json.load(open('./table_info.json', 'r'))
except:
    table_info = {}





tables = {}
tables['geometry'] = {
    'data':cc.read('wri_countries_a'),
    'config_options':{}
}
tables['country_aliases'] = {
    'data':cc.read('rw_aliasing_countries'),
    'config_options':{}
}

for rw_id, api_id in long_forms.items():
    logging.info(rw_id)

    # Fetch table name
    try:
        table_name = carto_data.loc[api_id]['table_name']
    except:
        msg = '***** ' + rw_id + ' not in carto_data *****'
        table_info[table_name] = {
            'error':msg
        }
        logging.info(msg)


    if table_name in table_info:
        if 'config_options' in table_info[table_name]:
            if 'prefixes' in table_info[table_name]['config_options']:
                if table_info[table_name]['config_options']['prefixes'] in [[u'--'], [u'already long']]:
                    logging.info('not fetching data for ' + table_name)
                    continue

    # Fetch data
    try:
        tables[table_name] = {
            'data':cc.read(table_name)
        }
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





# Add config options
for name, info in tables.items():
    logging.info(name)
    logging.info(info['data'].head(1))
    print

    ## To assign prefixes, uncomment
    '''
    prefixes = raw_input('time column prefixes?')
    table_info[name]['config_options']['prefixes'] = [pfx.strip() for pfx in prefixes.split(',')]
    '''

    ## To determine whether column prefixes should be added as a new column, uncomment
    '''
    add_indicator_column = raw_input('Add prefix as indicator? type anything for True, leave blank for False')
    if add_indicator_column != '':
        table_info[name]['config_options']['add_ind'] = True
    else:
        table_info[name]['config_options']['add_ind'] = False
    '''

    ## To determine whether the column should have a _ after the prefix, uncomment
    '''
    _included = raw_input('Is there a _ after the prefix? type anything for True, leave blank for False')
    if _included != '':
        table_info[name]['config_options']['_incl'] = True
    else:
        table_info[name]['config_options']['_incl'] = False
    '''

    ## To determine identifying info column (likely country code), uncomment
    country_code = raw_input('country code column? leave blank if needs country code added')
    if country_code != '':
        table_info[name]['config_options']['country_code'] = country_code
    else:
        table_info[name]['config_options']['country_code'] = False

    country_name = raw_input('country name column? leave blank if needs country name added')
    if country_name != '':
        table_info[name]['config_options']['country_name'] = country_name
    else:
        table_info[name]['config_options']['country_name'] = False





# Use known prefixes to reformat tables
ignore_columns = ['the_geom', 'cartodb_georef_status', ]
def pick_id_cols(col, prefixes):
    if col in ignore_columns:
        return False
    for pfx in prefixes:
        if pfx+'_' in col:
            return False
    return True

def pick_value_cols(col, prefixes, use_, only=None):
    if use_:
        seen = [True if pfx+'_' in col else False for pfx in prefixes]
    else:
        seen = [True if pfx in col else False for pfx in prefixes]

    if only:
        if sum(seen) == 1:
            # Only one match, keep if it matches 'only'
            if seen[prefixes.index(only)]:
                return True
        elif sum(seen) > 1:
            # More than one match, keep longest if it matches 'only'
            seen_pfxs = [_[0] for _ in list(zip(prefixes,seen)) if _[1]]
            seen_lengths = [len(_) for _ in seen_pfxs]
            max_index = seen_lengths.index(max(seen_lengths))
            if prefixes.index(seen_pfxs[max_index]) == prefixes.index(only):
                return True
    else:
        if sum(seen):
            return True
    return False

def prepare_date(date, pfx, use_):
    if use_:
        return int(date[date.index(pfx) + len(pfx) + 1:])
    else:
        return int(date[date.index(pfx) + len(pfx):])





name = 'soc_015_adult_literacy_rate'
prefixes = table_info[name]['config_options']['prefixes']
use_ = table_info[name]['config_options']['_incl']
logging.info(prefixes)
info = tables[name]



id_cols = [col for col in info['data'].columns if pick_id_cols(col, prefixes)]

df = pd.DataFrame(columns=['variable', 'country_code'])

for pfx in prefixes:
    logging.info(pfx)
    value_cols = [col for col in info['data'].columns if pick_value_cols(col, prefixes, use_, only=pfx)]
    logging.info('value cols:')
    logging.info(value_cols)
    _df = pd.melt(info['data'], id_vars=id_cols, value_vars=value_cols)
    logging.info(_df.head())
    logging.info('variables pre formatting')
    logging.info(_df['variable'].unique())
    _df['variable'] = [prepare_date(date, pfx, use_) for date in _df['variable']]
    logging.info('variables post formatting')
    logging.info(_df['variable'].unique())

    col_names = list(_df.columns)
    col_names[col_names.index('value')] = pfx
    _df.columns = col_names

    logging.info('columns')
    logging.info(_df.columns)
    logging.info('joined variable column')
    logging.info(df['variable'])
    logging.info('new variable column to add')
    logging.info(_df['variable'])
    #logging.info(union
    #logging.info(len(union)
    df = df.merge(_df, on=['variable', 'country_code'], how='outer')
    logging.info('merged shape')
    logging.info(df.shape)
