import logging
import sys
import os
logging.basicConfig(stream=sys.stderr, level=logging.ERROR)

import pandas as pd
import requests as req

# If not running in nrt container, environ variables will not be set

from utilities import *

CARTO_TABLE = 'cli_025_mean_sea_level_rise'
CARTO_SCHEMA = OrderedDict([
    ('altimeter_type', 'varchar'),
    ('merged_file_cycle', 'varchar'),
    ('year_and_fraction', 'date'),
    ('num_obs', 'number'),
    ('num_weighted_obs', 'number'),
    ('gmsl_no_gia', 'number'),
    ('sd_gmsl_no_gia', 'number'),
    ('gmsl_gia', 'number'),
    ('sd_gmsl_gia', 'number'),
    ('gauss_filt_gmsl_gia', 'number'),
    ('gauss_filt_gmsl_gia_ann_signal_removed', 'number')
])

UID_FIELD = 'year_and_fraction'
TIME_FIELD = 'year_and_fraction'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

def fetchData():
    # Read the files that are on the FTP
    df = cli_025.fetchData()
    logging.info(df)

    return(None)

def parseData():
    return(None)

def uploadData():
    return(None)

def main():
    fetchData()
    parseData()
    uploadData()
