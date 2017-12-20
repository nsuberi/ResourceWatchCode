# 3rd party libraries
import logging
import subprocess
import datetime

def cloudProcess(localTif_loc, gs_loc, gee_props):
    """
    Inputs: 
    * location of the tif to upload
    * loc to upload to on google storage
    * properties to set on the gee_asset, gee_props
    ** gee_props should be a dictionary w/ at least six keys: 
    ** imageCollection, gee_asset_name, band_names, nodata_value, time_start, time_end
    
    Outputs: files in the correct places on gs, and gee
    
    Assumes: Collection of correct name already exists on GEE
    """
    assert(type(gee_props)==dict)
    assert(all([prop in gee_props.keys() for prop in ["imageCollection", "gee_asset_name", "band_names", "nodata_value", "time_start", "time_end"]]))
    
    loadToGoogleStorage(localTif_loc, gs_loc)
    loadToGEE(gs_loc, gee_props)

def loadToGoogleStorage(localTif_loc, gs_loc):
    cmd = ["gsutil", "cp", localTif_loc, gs_loc]
    logging.info(subprocess.check_output(cmd))
    logging.info(localTif_loc.split('/')[-1] + ' up on google storage')

def loadToGEE(gs_loc, gee_props):
    
    # Do I need to include the CRS with --crs CRS?
    cmd = ["earthengine", "upload", "image", "--force",
    "--asset_id", gee_props["gee_asset_name"], gs_loc,
    "--nodata_value", gee_props["nodata_value"],
    "--pyramiding_policy=mode",
    "--bands", gee_props["band_names"],
    "-p", "system:time_start="+gee_props["time_start"],
    "-p", "system:time_end="+gee_props["time_end"]]
    try:
        logging.info(subprocess.check_output(cmd))
        logging.info('GEE asset upload started for ' + gee_props["gee_asset_name"])
        logging.info('Check back to ensure ACL is set to public before attempting to connect to the back office')
    
    except:
        logging.error(gs_loc)
        logging.error(gee_props)
        logging.error("Unexpected error:" + str(sys.exc_info()[0]))
        

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
    