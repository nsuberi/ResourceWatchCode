import logging
import sys
import os
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil import parser
import time
import cartosql

### Constants
SOURCE_URL = ''
FILENAME_INDEX = -1
TIMEOUT = 300
ENCODING = 'utf-8'
STRICT = False

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
MAX_AGE = datetime.today() - timedelta(days=3650)
CLEAR_TABLE_FIRST = False

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
        cartosql.createIndex(table, time_field)
    return []

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

def makeRoomForNewData(table, schema, uidfield, max_rows, existing_ids, new_ids):
    '''
    Delete excess rows by count
    Candidate_ids should be drawn from the existing_ids pulled at the beginning of this procedure
    At each round of new_data, if existing_ids are deleted they will have been removed from candidate_ids
    Does not attempt to order the new data
    Return dropped ids
    '''
    num_dropped = 0
    num_new_rows = len(new_ids)
    seen_ids = existing_ids + new_ids

    if len(seen_ids) > max_rows:
        if max_rows > num_new_rows:
            # Can accomodate all new_ids
            drop_ids = existing_ids[(max_rows - num_new_rows):]
            drop_response = cartosql.deleteRowsByIDs(table, drop_ids, id_field=uidfield, dtype=schema[uidfield])

            leftover_ids = existing_ids[:(max_rows - num_new_rows)]
            overflow_ids = []
        else:
            # Cannot accommodate all new_ids
            drop_ids = existing_ids + new_ids[max_rows:]
            drop_response = cartosql.deleteRowsByIDs(table, drop_ids, id_field=uidfield, dtype=schema[uidfield])

            num_lost_new_data = num_new_rows - MAX_ROWS
            logging.warning("Drop all existing_ids, and enough oldest new ids to have MAX_ROWS number of final entries in the table.")
            logging.warning("{} new data values were lost.".format(num_lost_new_data))

            leftover_ids = []
            overflow_ids = new_ids[:max_rows]

        numdropped = drop_response.json()['total_rows']
        if numdropped > 0:
            logging.info('Dropped {} old rows'.format(numdropped))

    return(leftover_ids, new_data, overflow_ids)


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

    # Return the file name
    return(filename)

def tryRetrieveData(SOURCE_URL, filename, TIMEOUT, ENCODING):
    # Optional logic in case this request fails with "unable to decode" response
    start = time.time()
    DATA_RETRIEVED = False
    elapsed = 0
    resource_location = os.path.join(SOURCE_URL, filename)

    while existing < TIMEOUT:
        elapsed = time.time() - start
        try:
            with urllib.request.urlopen(resource_locations) as f:
                res_rows = f.read().decode(ENCODING).splitlines()
                DATA_RETRIEVED = True
                return(res_rows)
        except:
            logging.error("Unable to retrieve resource on this attempt.")
            time.sleep(5)

    if not DATA_RETRIEVED:
        logging.error("Unable to retrive resource before timeout of {} seconds".format(TIMEOUT))
        if STRICT:
            raise Exception("Unable to retrieve data from {}".format(resource_locations))
        else:
            return([])

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

def processData(SOURCE_URL, filename, existing_ids, max_rows):
    """
    Inputs: FTP SOURCE_URL and filename where data is stored, existing_ids not to duplicate
    Actions: Retrives data, dedupes and formats it, and adds to Carto table
    Output: Number of new rows added
    """
    # Totals, persist throughout any pagination in next step
    leftover_ids = existing_ids.copy()
    num_new = 0
    num_overflow = 0

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

                date = fix_datetime_UTC(row, dttm_elems = dttm_elems)

                UID = genUID('value_type', date)
                values = [UID, date, value, "value_type"]

                new_data = insertIfNew(UID, values, leftover_ids, new_data)
            else:
                logging.debug("Skipping row: {}".format(row))

    if len(new_data):
        # Check whether should delete to make room
        new_ids = list(new_data.keys())
        leftover_ids, new_ids, overflow_ids = makeRoomForNewData(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, max_rows, leftover_ids, new_ids)
        for overflow in overflow_ids:
            new_data.pop(overflow)

        num_overflow += len(overflow_ids)
        num_new += len(new_ids)
        new_data = list(new_data.values())
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data)

    ### End page/chunk processing

    num_leftover = len(leftover_ids)
    return(num_leftover, num_new, num_overflow)

###
## Processing data for Carto
###

def genUID(value_type, value_date):
    return("_".join([str(value_type), str(value_date)]).replace(" ", "_"))

def formatDateFunction(unformatteddate):
    # DO STUFF
    formatted_date = unformatteddate
    return(formatted_date)

### Standardizing datetimes

def fix_datetime_UTC(row, construct_datetime_manually=True,
                     dttm_elems={},
                     dttm_columnz=None,
                     dttm_pattern="%Y-%m-%d %H:%M:%S"):
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
    if construct_datetime_manually:
        assert(type(dttm_elems)==dict)
        assert(dttm_columnz==None)

        if "year_ix" in dttm_elems:
            year = int(row[dttm_elems["year_ix"]])
        else:
            year = 1900
            logging.warning("Default year set to 1900")

        if "month_ix" in dttm_elems:
            month = int(row[dttm_elems["month_ix"]])
        else:
            month = 1
            logging.warning("Default mon set to January")

        if "day_ix" not in dttm_elems:
            day = int(row[dttm_elems["day_ix"]])
        else:
            day = 1
            logging.warning("Default day set to first of month")

        dt = datetime(year=year,month=month,day=day)
        if "hour_ix" in dttm_elems:
            dt = dt.replace(hour=int(row[dttm_elems["hour_ix"]]))
        if "min_ix" in dttm_elems:
            dt = dt.replace(minute=int(row[dttm_elems["min_ix"]]))
        if "sec_ix" in dttm_elems:
            dt = dt.replace(second=int(row[dttm_elems["sec_ix"]]))
        if "milli_ix" in dttm_elems:
            dt = dt.replace(milliseconds=int(row[dttm_elems["milli_ix"]]))
        if "micro_ix" in dttm_elems:
            dt = dt.replace(microseconds=int(row[dttm_elems["micro_ix"]]))
        if "tzinfo_ix" in dttm_elems:
            timezone = pytz.timezone(str(row[dttm_elems["tzinfo_ix"]]))
            dt = timezone.localize(dt)

        formatted_date = dt.strftime(dttm_pattern)
    else:
        # Make sure dttm_columnz was provided
        assert(dttm_columnz!=None)
        default_date = datetime(year=1990, month=1, day=1)
        # If dttm_columnz is not a list, it must be a single list index, type int
        if type(dttm_columnz) != list:
            assert(type(dttm_columns) == int)
            formatted_date = parser.parse(row[dttm_columnz], default=default_date).strftime(dttm_pattern)
            # Need to provide the default parameter to parser.parse so that missing entries don't default to current date

        elif len(dttm_columnz)>=1:
            # Concatenate these entries with a space in between, use dateutil.parser
            dttm_contents = " ".join([row[col] for col in dttm_columnz])
            formatted_date = parser.parse(dttm_contents, default=default_date).strftime(dttm_pattern)

    return(formatted_date)


'''
Options include:

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def decimalToDatetime(dec, date_pattern="%Y-%m-%d %H:%M:%S"):
    """
    Convert a decimal representation of a year to a desired string representation
    I.e. 2016.5 -> 2016-06-01 00:00:00
    """
    dec = float(dec)
    year = int(dec)
    rem = dec - year
    base = datetime(year, 1, 1)
    dt = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    result = dt.strftime(date_pattern)
    return(result)
'''

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    if CLEAR_TABLE_FIRST:
        cartosql.dropTable(CARTO_TABLE)

    ### 1. Check if table exists, if not, create it
    checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    ### 2. Delete old rows
    num_expired = cleanOldRows(CARTO_TABLE, TIME_FIELD, MAX_AGE)

    ### 3. Retrieve existing data
    r = cartosql.getFields(id_field, table, order='{} desc'.format(time_field), f='csv')
    existing_ids = r.text.split('\r\n')[1:-1]
    num_existing = len(existing_ids)

    logging.debug("First 10 IDs already in table: {}".format(existing_ids[:10]))

    ### 4. Fetch data from FTP, dedupe, process
    filename = fetchDataFileName(SOURCE_URL)
    num_leftover, num_new, num_overflow = processData(SOURCE_URL, filename, existing_ids, MAX_ROWS)

    ### 5. Notify results
    num_overwritten = num_existing - num_leftover
    logging.info('Expired rows: {}, Previous rows: {}, New rows: {}, Overwritten rows: {}, Max: {}'.format(num_expired, num_existing, num_new, num_overwritten, MAX_ROWS))

    ###
    logging.info("SUCCESS")
