import pandas as pd
import os
import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.ERROR)

import requests as req
from collections import OrderedDict

# Utilities
import carto
import misc


UNHCR_DATA = "resourcewatch/UNHCR_data_trial/"

def fetch_unhcr_data(codes_and_names):
    
    monthly_asylumn_url = "http://popdata.unhcr.org/api/stats/asylum_seekers_monthly.json?year={year}"
    poc_annual_url = "http://popdata.unhcr.org/api/stats/persons_of_concern_all_countries.json?year={year}"
    
    for yr in range(1999,2018):
        url = "http://popdata.unhcr.org/api/stats/asylum_seekers_monthly.json?year={year}"
        url = url.format(year=yr)
        res = req.get(url)
        data = res.json()
        df = pd.DataFrame(data)

        group_orig = df.groupby(["country_of_origin", "month", "year"]).sum().reset_index()
        group_dest = df.groupby(["country_of_asylum", "month", "year"]).sum().reset_index()

        group_orig.columns = ["country", "month", "year", "asylum_seekers_fleeing"]
        group_dest.columns = ["country", "month", "year", "asylum_seekers_arriving"]

        join = group_orig.join(group_dest.set_index(["country", "month", "year"]), on=["country", "month", "year"], how="outer")
        join = join.fillna(0)

        logging.info(yr, group_orig.shape)
        logging.info(group_orig.head())
        logging.info(yr, group_dest.shape)
        logging.info(group_dest.head())
        logging.info(join.shape)
        logging.info(join.head())

        # Fix month / year to UTC

        # Upload to Carto
   
    for yr in range(1990,2018):
        url = "http://popdata.unhcr.org/api/stats/persons_of_concern_all_countries.json?year={year}"
        url = url.format(year=yr)
        res = req.get(url)
        data = res.json()
        df = pd.io.json.json_normalize(data)
        df = df.fillna(0)
        
        logging.info(yr, df.shape)
        logging.info(df.head())

        # Fix years

        # Upload to Carto

    
    

def main():
    # Upload history of data
    poc_data, asylum_seeker_data = fetch_unhcr_data()

    # Write to S3
    misc.write_to_S3(poc_data, UNHCR_DATA  + "poc_data.csv")
    misc.write_to_S3(asylum_seeker_data, UNHCR_DATA  + "poc_data.csv")

    # Write to Carto
    CARTO_TABLE_POC = "soc_xxx_persons_of_concern_data"
    CARTO_TABLE_ASY = "soc_xxx_monthly_asylum_seekers_data"

    CARTO_SCHEMA_POC = OrderedDict([
        ('ISO3', 'text'),
        ('Country', 'text'),
        ('Year', 'timestamp'),
        (val_name, 'numeric'),
        ('Units','text')
    ])
    
    CARTO_SCHEMA_ASY = OrderedDict([
        ('ISO3', 'text'),
        ('Country', 'text'),
        ('Year', 'timestamp'),
        (val_name, 'numeric'),
        ('Units','text')
    ])

    ### 1. Check if table exists and create table, if it does, drop and replace
    #dest_ids = []
    if not carto.tableExists(CARTO_TABLE):
        logging.info('Table {} does not exist'.format(CARTO_TABLE))
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)
        else:
            carto.dropTable(CARTO_TABLE)
            carto.createTable(CARTO_TABLE, CARTO_SCHEMA)

            ### 2. Insert new observations
            # https://stackoverflow.com/questions/19585280/convert-a-row-in-pandas-into-list
            rows = long_form.values.tolist()
            logging.error(rows[:10])
            if len(rows):
                carto.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, rows)

main()