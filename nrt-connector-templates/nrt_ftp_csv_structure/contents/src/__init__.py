import logging
import sys
import os
import time
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil import parser
import cartosql

### Constants
SOURCE_URL = ''
FILENAME_INDEX = -1
TIMEOUT = 300
ENCODING = 'utf-8'
STRICT = False
CLEAR_TABLE_FIRST = False

### Table name and structure
CARTO_TABLE = ''
CARTO_SCHEMA = OrderedDict([
        ('UID', 'text'),
        ('date', 'timestamp'),
        ('value', 'numeric'),
        ('value_type', 'text')
    ])
UID_FIELD = 'UID'
TIME_FIELD = 'date'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Table limits
MAX_ROWS = 1000000
MAX_AGE = datetime.today() - timedelta(days=365*150)

###
## Carto code
###

def checkCreateTable(table, schema, id_field, time_field):
    '''
    Get existing ids or create table
    Return a list of existing ids in time order
    '''
    if cartosql.tableExists(table):
        logging.info('Table {} already exists'.format(table))
    else:
        logging.info('Creating Table {}'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if id_field != time_field:
            cartosql.createIndex(table, time_field)

def cleanOldRows(table, time_field, max_age, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Delete excess rows by age
    Max_Age should be a datetime object or string
    Return number of dropped rows
    '''
    num_expired = 0
    if cartosql.tableExists(table):
        if isinstance(max_age, datetime):
            max_age = max_age.strftime(date_format)
        elif isinstance(max_age, str):
            logging.error('Max age must be expressed as a datetime.datetime object')

        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_expired = r.json()['total_rows']
    else:
        logging.error("{} table does not exist yet".format(table))

    return(num_expired)

def deleteExcessRows(table, max_rows, time_field):
    '''Delete rows to bring count down to max_rows'''
    num_dropped=0
    # 1. get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 2. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return(num_dropped)

###
## Accessing remote data
###

def fetchDataFileName(SOURCE_URL):
    """
    Select the appropriate file from FTP to download data from
    """
    with urllib.request.urlopen(SOURCE_URL) as f:
        ftp_contents = f.read().decode('utf-8').splitlines()

    filename = ''
    ALREADY_FOUND=False
    for fileline in ftp_contents:
        fileline = fileline.split()
        logging.debug("Fileline as formatted on server: {}".format(fileline))
        potential_filename = fileline[FILENAME_INDEX]

        ###
        ## Set conditions for finding correct file name for this FTP
        ###

        if (potential_filename.endswith(".txt") and ("V4" in potential_filename)):
            if not ALREADY_FOUND:
                filename = potential_filename
                ALREADY_FOUND=True
            else:
                logging.warning("There are multiple filenames which match criteria, passing most recent")
                filename = potential_filename

    logging.info("Selected filename: {}".format(filename))
    if not ALREADY_FOUND:
        logging.warning("No valid filename found")

    return(filename)

def tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING):
    # Optional logic in case this request fails with "unable to decode" response
    start = time.time()
    elapsed = 0
    resource_location = os.path.join(SOURCE_URL, filename)

    while elapsed < TIMEOUT:
        elapsed = time.time() - start
        try:
            with urllib.request.urlopen(resource_location) as f:
                res_rows = f.read().decode(ENCODING).splitlines()
                return(res_rows)
        except:
            logging.error("Unable to retrieve resource on this attempt.")
            time.sleep(5)

    logging.error("Unable to retrive resource before timeout of {} seconds".format(TIMEOUT))
    if STRICT:
        raise Exception("Unable to retrieve data from {}".format(resource_locations))
    return([])



def genUID(value_type, value_date):
    return("_".join([str(value_type), str(value_date)]).replace(" ", "_"))

def formatDateFunction(unformatteddate):
    # DO STUFF
    formatted_date = unformatteddate
    return(formatted_date)




def insertIfNew(newUID, newValues, existing_ids, new_data):
    '''
    For new UID, values, check whether this is already in our table
    If not, add it
    Return new_ids and new_data
    '''
    seen_ids = existing_ids + list(new_data.keys())
    if newUID not in seen_ids:
        new_data[newUID] = newValues
        logging.debug("Adding {} data to table".format(newUID))
    else:
        logging.debug("{} data already in table".format(newUID))
    return(new_data)

def processData(SOURCE_URL, filename, existing_ids):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    num_new = 0

    ### Specific to each page/chunk in data processing

    res_rows = tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING)
    new_data = {}
    for row in res_rows:
        ###
        ## CHANGE TO REFLECT CRITERIA FOR KEEPING ROWS FROM THIS DATA SOURCE
        ###
        if not (row.startswith("HDR")):
            row = row.split()
            ###
            ## CHANGE TO REFLECT CRITERIA FOR KEEPING ROWS FROM THIS DATA SOURCE
            ###
            if len(row)==len(CARTO_SCHEMA):
                logging.debug("Processing row: {}".format(row))
                # Pull data available in each line
                VALUE_INDEX = 3
                value = row[VALUE_INDEX]

                # Pull times associated with those data
                dttm_elems = {
                    "year_ix":0,
                    "month_ix":1,
                    "day_ix":2
                }

                date = datetime(year=int(row[0]),
                                month=int(row[1]),
                                day=int(row[2])).strftime("%Y-%m-%d")

                UID = genUID('value_type', date)
                values = [UID, date, value, "value_type"]

                new_data = insertIfNew(UID, values, existing_ids, new_data)
            else:
                logging.debug("Skipping row: {}".format(row))

    if len(new_data):
        num_new += len(new_data)
        new_data = list(new_data.values())
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)

    ### End page/chunk processing

    return(num_new)

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    if CLEAR_TABLE_FIRST:
        logging.info("clearing table")
        cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists, if not, create it
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ### 2. Delete old rows
    num_expired = cleanOldRows(CARTO_TABLE, TIME_FIELD, MAX_AGE)

    ### 3. Retrieve existing data
    r = cartosql.getFields(UID_FIELD, CARTO_TABLE, order='{} desc'.format(TIME_FIELD), f='csv')
    existing_ids = r.text.split('\r\n')[1:-1]
    num_existing = len(existing_ids)

    logging.debug("First 10 IDs already in table: {}".format(existing_ids[:10]))

    ### 4. Fetch data from FTP, dedupe, process
    filename = fetchDataFileName(SOURCE_URL)
    num_new = processData(SOURCE_URL, filename, existing_ids)

    ### 5. Delete data to get back to MAX_ROWS
    num_deleted = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    ### 6. Notify results
    logging.info('Expired rows: {}, Previous rows: {},  New rows: {}, Dropped rows: {}, Max: {}'.format(num_expired, num_existing, num_new, num_deleted, MAX_ROWS))
    logging.info("SUCCESS")
