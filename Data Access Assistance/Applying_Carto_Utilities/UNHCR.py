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
    indicators = list(codes_and_names.keys())
    for ix, indicator in enumerate(indicators):
        logging.info(indicator)
        value_name = codes_and_names[indicator]['column_name']
        
        # Fetch data
        res = req.get("http://api.worldbank.org/countries/all/indicators/{}?format=json&per_page=10000".format(indicator))
        logging.info(res.text)
        
        # Format into dataframe, only keep some columns
        data = pd.io.json.json_normalize(res.json()[1])
        data = data[["country.value", "date", "value"]]
        data.columns = ["Country", "Year", value_name]
        # Standardize year column for ISO time
        data["Year"] = misc.fix_datetime_UTC(data, dttm_elems={"year_col":"Year"})
        # Only keep countries, not larger political bodies
        data = data.iloc[misc.pick_wanted_entities(data["Country"].values)]
        # Set index to Country and Year
        data = data.set_index(["Country", "Year"])
   
        if ix == 0:
            # Start off the dataframe
            all_world_bank_data = data
        else:
            # Continue adding to the dataframe
            all_world_bank_data = all_world_bank_data.join(data, how="outer")

    # Finished fetching, reset_index
    all_world_bank_data = all_world_bank_data.reset_index()
    
    # Add ISO3 column - will now only contained desired entities
    all_world_bank_data["ISO3"] = list(map(misc.add_iso, all_world_bank_data["Country"]))
    # Drop rows which don't have an ISO3 assigned
    all_world_bank_data = all_world_bank_data.loc[pd.notnull(all_world_bank_data["ISO3"])]
    # Set the index to be everything except the value column. This simplifies dissection later
    all_world_bank_data = all_world_bank_data.set_index(["Country", "ISO3", "Year"])
    
    return(all_world_bank_data)

def main():

    ## World Bank data series codes and names
    # key = WB code: https://datahelpdesk.worldbank.org/knowledgebase/articles/201175-how-does-the-world-bank-code-its-indicators
    # value = [table_name, value_column_name, units]
    
    
    ### WARNINGS
    # For this to work as expected, there must not be any , in the table names
    # And table names cannot be longer than a certain number of characters, equal to
    # Length of: soc_106_proportion_of_seats_held_by_women_in_national_parliamen (63 characters)
    data_codes_and_names = {
        'EG.ELC.ACCS.ZS': {'table_name': 'soc_100 Access to electricity', 
                           'column_name': 'acc_to_elec', 
                           'unit': '% of population'}, 
        'EG.FEC.RNEW.ZS': {'table_name': 'soc_101 Renewable energy consumption', 
                           'column_name': 'rnw_ene_con', 
                           'unit': '% of total final energy consumption'}, 
        'IT.NET.USER.ZS': {'table_name': 'soc_102 Individuals using the Internet', 
                           'column_name': 'intnt_use', 
                           'unit': '% of population'}, 
        'NE.CON.PRVT.PC.KD': {'table_name': 'soc_103 Household final consumption expenditure per capita', 
                              'column_name': 'cons_exp', 
                              'unit': 'constant 2010 US$'}, 
        'NV.IND.TOTL.KD': {'table_name': 'soc_104 Industry value added', 
                           'column_name': 'ind_val_add', 
                           'unit': 'constant 2010 US$'}, 
        'NY.GDP.TOTL.RT.ZS': {'table_name': 'soc_105 Total natural resources rents', 
                              'column_name': 'nat_rsc_rnt', 
                              'unit': '% of GDP'}, 
        'SG.GEN.PARL.ZS': {'table_name': 'soc_106 Proportion of women in national parliaments',
                           'column_name': 'wmn_prlmnt', 
                           'unit': '% of parliamentary seats'}, 
        'SL.EMP.TOTL.SP.ZS': {'table_name': 'soc_107 Employment to population ratio', 
                              'column_name': 'empl_ratio', 
                              'unit': '% employed population, ages 15+'}, 
        'SM.POP.NETM': {'table_name': 'soc_108 Net migration', 
                        'column_name': 'net_migr', 
                        'unit': 'number of net in-migrants'}, 
        'SP.DYN.LE00.IN': {'table_name': 'soc_109 Life expectancy at birth', 
                           'column_name': 'life_exp', 
                           'unit': 'years'}, 
        'SP.URB.TOTL.IN.ZS': {'table_name': 'soc_110 Urban population', 
                              'column_name': 'urban_pop', 
                              'unit': '% of total population'}, 
        'TM.VAL.MRCH.CD.WT': {'table_name': 'soc_111 Merchandise imports', 
                              'column_name': 'merch_imp', 
                              'unit': 'current US$'}, 
        'NY.GDP.MKTP.CD': {'table_name': 'soc_112 GDP', 
                           'column_name': 'gdp', 
                           'unit': 'current US$'}}
    
    all_world_bank_data = fetch_wb_data(data_codes_and_names)

    # Write to S3 and Carto the individual data sets
    for code, info in data_codes_and_names.items():
        val_name = info['column_name']
        # Can't have spaces in Carto table names
        table = info['table_name'].replace(' ', '_').lower()
        units = info['unit']
        
        long_form = all_world_bank_data[val_name]
        long_form = long_form.reset_index()
        long_form['Units'] = units
        
        column_order = ['ISO3', 'Country', 'Year', val_name, 'Units']
        
        # Enforce order in the data frame
        long_form = long_form[column_order]
        
        long_form = long_form[pd.notnull(long_form[val_name])]
        
        # Write to S3
        misc.write_to_S3(long_form, WB_DATA + "wb_data_long_{}.csv".format(val_name))

        # Write to Carto
        CARTO_TABLE = table

        CARTO_SCHEMA = OrderedDict([
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