import logging
import sys
import os
logging.basicConfig(stream=sys.stderr, level=logging.ERROR)

import pandas as pd
import requests as req

in_nrt_container = os.environ.get('IN_NRT_CONTAINER', False)

# If not running in nrt container, environ variables will not be set
if not in_nrt_container:
    from configparser import ConfigParser
    config = ConfigParser()
    config.read("/Users/nathansuberi/Desktop/Code Portfolio/ResourceWatchCode/.env")
    os.environ["CARTO_USER"] = "rw-nrt"
    os.environ["CARTO_KEY"] = config.get("auth", "carto_api_token_nrt")

from utilities import *

CARTO_TABLE = 'test_floodreports'
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_UID', 'text'),
    ('ID', 'int'),
    ('GlideNumber', 'text'),
    ('Country', 'text'),
    ('OtherCountry', 'text'),
    ('long', 'numeric'),
    ('lat', 'numeric'),
    ('Area', 'numeric'),
    ('Began', 'timestamp'),
    ('Ended', 'timestamp'),
    ('Validation', 'text'),
    ('Dead', 'int'),
    ('Displaced', 'int'),
    ('MainCause', 'text'),
    ('Severity', 'numeric')
])
UID_FIELD = '_UID'
TIME_FIELD = 'Began'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')



def fetchData():
    # Read the files that are on the FTP
    ftp = "ftp://podaac.jpl.nasa.gov/allData/merged_alt/L2/TP_J1_OSTM/global_mean_sea_level/"
    df = pd.DataFrame(req.urlopen(ftp).read().splitlines())
    df["files"] = df[0].str.split(expand=True)[8].astype(str)
    logging.info(df["files"])
    df["files"] = df["files"].apply(lambda row: row[2:-1])
    # Select the file that contains the data... i.e. ends with .txt, and has "V4" in the name
    data_file_index = df["files"].apply(lambda row: row.endswith(".txt") & ("V4" in row))
    logging.info(data_file_index)
    # Pull out just the file name
    remote_file_name = df.loc[data_file_index,"files"].values[0]
    logging.info(remote_file_name)

    sea_level = pd.read_csv(ftp+remote_file_name, header = None, sep = '\t')

    df = sea_level
    df = df[df[0] != 'HDR']
    df = df[~df[0].astype(str).str.contains('HDR')]
    df = df[~df[0].astype(str).str.contains('999')]

    return(None)

def parseData():
    return(None)

def uploadData():
    return(None)

def main():
    fetchData()
    parseData()
    uploadData()
