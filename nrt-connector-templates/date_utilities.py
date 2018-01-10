from dateutil import parser
import pytz
import datetime
from datetime import timedelta, datetime

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

def recentEnough(date, MAX_AGE):
    '''Assume date is a string, MAX_AGE a datetime'''
    return(parser.parse(date) > MAX_AGE)

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

def retrieve_formatted_dates(nc, time_var_name, date_pattern=DATE_FORMAT):
    '''
    Inputs:
    * pointer to a netcdf file
    * name of the time variable
    Outputs:
    * dates formatted according to DATE_FORMAT
    '''
    # Extract time variable range
    nc = Dataset(nc)
    time_displacements = nc[time_var_name]
    # Clean up reference to nc object
    del nc

    # Identify time units
    # fuzzy=True allows the parser to pick the date out from a string with other text
    time_units = time_displacements.getncattr('units')
    logging.debug("Time units: {}".format(time_units))
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.debug("Reference time: {}".format(ref_time))

    # Format times to DATE_FORMAT
    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    logging.debug('Dates available: {}'.format(formatted_dates))
    return(formatted_dates)

# https://stackoverflow.com/questions/6999726/how-can-i-convert-a-datetime-object-to-milliseconds-since-epoch-unix-time-in-p
def format_time_for_gee(time_start, time_end, orig_date_pattern="%Y-%m-%d"):
    """
    Inputs: some times as strings, and a date_pattern they correspond to
    Outputs: those times as UNIX time
    """
    # Set epoch to measure against
    epoch = datetime.datetime.utcfromtimestamp(0)
    # Convert time_start and time_end to UTC... not clear what their original time zone was
    time_start = time_start
    time_start = time_start

    # Convert the difference of time_start and time_end from the last epoch to milliseconds
    time_start = (datetime.datetime.strptime(time_start, orig_date_pattern)-epoch).total_seconds()*1000.0
    time_end = (datetime.datetime.strptime(time_end, orig_date_pattern)-epoch).total_seconds()*1000.0

    return(time_start, time_end)
