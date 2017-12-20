# Libraries to fetch data
from urllib.request import urlopen
import shutil
from contextlib import closing
import gzip

# Libraries to handle data
from netCDF4 import Dataset
import rasterio

# Library to interact with OS
import subprocess
import os
import glob

# Libraries to reformat data
#from misc import fix_datetime_UTC
import datetime
from dateutil import parser
import numpy as np # use to set data type for rasterio
np.set_printoptions(threshold='nan')

# Libraries to debug
import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# Script options
PROCESS_FULL_HISTORY = False
PROCESS_PARTIAL_HISTORY = False
PARTIAL_HISTORY_LENGTH = 120

###
## Procedure for obtaining the netcdf file, and processing it to tifs
###

def download_full_nc_history(tmpNcFolder):
    """
    Inputs: location to store nc file temporary
    Outputs: Newest available gistemp250 file name, along with time_start and time_end for the entire collection
    """
    remote_path = 'https://data.giss.nasa.gov/pub/gistemp/'
    ncFile_zipped = 'gistemp250.nc.gz'
    ncFile_name = tmpNcFolder + ncFile_zipped[:-3]

    local_path = os.getcwd()

    logging.info(remote_path)
    logging.info(ncFile_zipped)
    logging.info(ncFile_name)

    #Download the file .nc
    with closing(urlopen(remote_path + ncFile_zipped)) as r:
        with gzip.open(r, "rb") as unzipped:
            with open(ncFile_name, 'wb') as f:
                shutil.copyfileobj(unzipped, f)
    
    logging.info('Downloaded full nc history')
    
    # NEED TO READ TIME_START FROM THE DATA... is in metadata?
    #time_start = fix_datetime_UTC("")
    
    #today = datetime.datetime.now()
    #time_end = fix_datetime_UTC(today)
    nc = Dataset(ncFile_name)
    return (nc)

def create_formatted_dates(ref_time, time_displacements, date_pattern="%Y-%m-%d"):
    """
    Inputs:
    * ref_time in datetime.datetime format
    * list of time values corresponding to data in the nc file
    ** time values are expressed in days since the ref_time
    Outputs:
    * list of strings in desired date_pattern
    """
    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    return(formatted_dates)
    

def process_full_history_to_tifs(nc, time_var_name, data_var_name,
                                 tmpTifFolder, tifFileName_stub):
    # Extract time variable range
    time_displacements = nc[time_var_name]
    num_time_steps = len(time_displacements)
    logging.info(num_time_steps)
    
    # Identify time units
    # fuzzy=True allows the parser to pick the date out from a string with other text
    time_units = time_displacements.getncattr('units')
    logging.info(time_units)
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.info(ref_time)
    
    # Create dates ready for tif names
    formatted_dates = create_formatted_dates(ref_time, time_displacements)
    
    # Convert nc to tifs
    netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_dates)
    
def process_partial_history_to_tifs(nc, time_var_name, data_var_name,
                                 tmpTifFolder, tifFileName_stub, num_to_keep):
    # Extract time variable range
    time_displacements = nc[time_var_name]
    num_time_steps = len(time_displacements)
    logging.info(num_time_steps)
    
    # Identify time units
    # fuzzy=True allows the parser to pick the date out from a string with other text
    time_units = time_displacements.getncattr('units')
    logging.info(time_units)
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.info(ref_time)
    
    # Create dates ready for tif names
    formatted_dates = create_formatted_dates(ref_time, time_displacements[-num_to_keep:])
    
    # Convert nc to tifs
    netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_dates)
    
def process_most_recent_to_tif(nc, time_var_name, data_var_name,
                               tmpTifFolder, tifFileName_stub):
    ### TO DO
    ## Check to see if this is a new addition
    ## For now, simply overwrite
    ###
    
    # Extract time variable range
    time_displacement = nc[time_var_name]
    
    # Identify time units
    # fuzzy=True allows the parser to pick the date out from a string with other text
    time_units = time_displacement.getncattr('units')
    logging.info(time_units)
    ref_time = parser.parse(time_units, fuzzy=True)
    logging.info(ref_time)
    
    # Create date ready for tif name
    formatted_date = create_formatted_dates(ref_time, [time_displacement[-1]])
    
    # Convert nc to tif
    netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_date)

def netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_datez):
    """
    Inputs: 
    * pointer to a netcdf file, nc
    * variable name to select from the nc
    * folder to place temporary TIFFS
    * base for the tif file names
    * list of formatted_datez corresponding to entries in the nc file... must be a list
    
    Outputs:
    * Formatted TIFF files ready for GEE in tmpTifFolder
    """
    assert(type(formatted_datez)==list)
    for time_step, date in enumerate(formatted_datez):
        # Intercept the case where we're only looking at the most recent observation,
        # not the entire history
        
        if PROCESS_FULL_HISTORY:
            time_step=time_step
        elif PROCESS_PARTIAL_HISTORY:
            time_step=-(len(formatted_datez)+time_step)
        else:
            time_step = -1
            
        data = nc[data_var_name][time_step,:,:]
        tifFile_name = tmpTifFolder + tifFileName_stub + date + "_.tif"

        #data[data < -40] = -99
        #data[data > 40] = -99
        # This was causing an error?
        #print (data)

        # Return lat info
        south_lat = -90
        north_lat = 90
        # Return lon info
        west_lon = -180
        east_lon = 180

        # Transformation function
        transform = rasterio.transform.from_bounds(west_lon, south_lat, east_lon, north_lat, data.shape[1], data.shape[0])
        # Profile
        profile = {
            'driver':'GTiff', 
            'height':data.shape[0], 
            'width':data.shape[1], 
            'count':1, 
            'dtype':np.float64, 
            'crs':'EPSG:4326', 
            'transform':transform, 
            'compress':'lzw', 
            'nodata':-99
        }

        with rasterio.open(tifFile_name, 'w', **profile) as dst:
            dst.write(data.astype(profile['dtype']), 1)

        logging.info('netCDF converted to TIFF' + tifFile_name)

    
###
## Procedure for moving tif files to the cloud
### 

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
    
def process_tif_files_to_cloud(tmpTifFolder, cloud_props, asset_props):
    """
    Inputs:
    * folder with tif files to loop over
    * cloud_props, which at least contain keys: imageCollection, gs_bucket
    * asset_props, which at least contain keys: nodata_val, band_names
    Outputs: files in the correct places on gs and gee
    """
    assert(type(cloud_props)==dict)
    assert(all([prop in cloud_props.keys() for prop in ["imageCollection", "gs_bucket"]]))
    
    # Create collection if doesn't already exist
    cmd = ["earthengine", "create", "collection",
           "users/resourcewatch/" + cloud_props["imageCollection"]]
    logging.info(subprocess.check_output(cmd))

    tifs = glob.glob(tmpTifFolder + "*_.tif")
    
    for ix, tif in enumerate(tifs):
        
        # read time_start from the file name
        # [-15:-5] is derived from the file name convention
        if ix < (len(tifs)-1):
            time_start = tif[-15:-5]
            time_end = tifs[ix+1][-15:-5]
        else:
            time_start = tif[-15:-5]
            ### TO DO
            ## Adjust this... should just increment the month by 1, accounting for year overflow
            ## 
            time_end = (datetime.datetime.strptime(time_start,"%Y-%m-%d") + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
        
        # Times need to be expressed in milliseconds since last epoch, UNIX time
        # https://en.wikipedia.org/wiki/Unix_time
        # https://developers.google.com/earth-engine/glossary
        # See discussion here: https://groups.google.com/forum/#!searchin/google-earth-engine-developers/Value$20for$20property$20$27system$3Atime_start$27$20must$20be$20a$20number.%7Csort:date/google-earth-engine-developers/OG-G_7JzQGA/rnf-9oOIGwAJ
        # Livia.p's comment on 11/7/16
        time_start, time_end = format_time_for_gee(time_start, time_end)
        
        ## Some name formatting issues:
        # This isolates the tif name
        tifFile_name = tif.split("/")[-1]
        # The [:-4] below strips .tif from the asset name
        assetName = tifFile_name[:-5]
        kwargs = {
            "localTif_loc":tif,
            "gs_loc":"gs://"+cloud_props["gs_bucket"]+"/raster/"+cloud_props["imageCollection"]+"/"+tifFile_name,
            "gee_props":{
                "imageCollection":cloud_props["imageCollection"],
                "gee_asset_name": "users/resourcewatch/" + cloud_props["imageCollection"] + "/" + assetName,
                "band_names":asset_props["band_names"],
                "nodata_value":asset_props["nodata_val"],
                "time_start":str(int(time_start)),
                "time_end":str(int(time_end))
            }
        }

        cloudProcess(**kwargs)
    
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
    
###
## Cleaning up
###
    
def cleanUp(tmpDataFolder):
    shutil.rmtree(tmpDataFolder)
    
    
###
## Execution
### 

def main():
    
    logging.info('starting')
    
    # Create a temporary folder structure to store data
    tmpDataFolder = "tmpData"
    try:
        cleanUp(tmpDataFolder)
        os.mkdir(tmpDataFolder)
    except:
        os.mkdir(tmpDataFolder)
    logging.info("Clean folder created")
    
    tmpNcFolder = tmpDataFolder + "/ncFiles/"
    tmpTifFolder = tmpDataFolder + "/tifFiles/"
    os.mkdir(tmpNcFolder)
    os.mkdir(tmpTifFolder)
    
    # Returns the entire history of GISTEMP in a netCDF file
    nc = download_full_nc_history(tmpNcFolder)
    time_var_name = 'time'
    data_var_name = 'tempanomaly'
    nodata_val = str(nc[data_var_name].getncattr("_FillValue"))
    band_names = "surface_temp_anomalies"
    
    # Populate the tmpTifFolder will all files to process
    tifFileName_stub = "cli_035_surface_temp_analysis_"
    if PROCESS_FULL_HISTORY:
        process_full_history_to_tifs(nc, time_var_name, data_var_name, 
                                     tmpTifFolder, tifFileName_stub)
    elif PROCESS_PARTIAL_HISTORY:
        process_partial_history_to_tifs(nc, time_var_name, data_var_name, 
                                   tmpTifFolder, tifFileName_stub, PARTIAL_HISTORY_LENGTH)
    else:
        process_most_recent_to_tif(nc, time_var_name, data_var_name, 
                                   tmpTifFolder, tifFileName_stub)
    
    # Process all files in the tmpTifFolder onto the cloud
    cloud_props = {
        "imageCollection": "cli_035_surface_temp_analysis",
        "gs_bucket": "resource-watch-public"
    }
    asset_props = {
        "nodata_val":nodata_val,
        "band_names":band_names
    }
    process_tif_files_to_cloud(tmpTifFolder, cloud_props, asset_props)
    
    # Clean up before exit
    #cleanUp(tmpDataFolder)

    logging.info('container process finished, container cleaned')
    
main()