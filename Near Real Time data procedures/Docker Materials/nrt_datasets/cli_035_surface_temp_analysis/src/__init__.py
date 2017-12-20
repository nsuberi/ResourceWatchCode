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

# Libraries to reformat data
from misc import fix_datetime_UTC
import datetime
import numpy as np # use to set data type for rasterio
np.set_printoptions(threshold='nan')

# Libraries to debug
import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# Script options
PROCESS_HISTORY = True


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

def process_full_history_to_tifs(nc, var_name, tmpTifFolder, tifFileName_stub):
    # Time range stored in first index
    time_range = nc[var_name].shape[0]
    logging.info(time_range)
    for time_step in range(time_range):
        netcdf2tif(nc, var_name, tifFileName_stub, time_step)
    
def process_most_recent_to_tif(nc, var_name, tmpTifFolder, tifFileName_stub):
    ### TO DO
    ## Check to see if this is a new addition
    ###
    netcdf2tif(nc, var_name, tifFileName_stub, -1)

def netcdf2tif(nc, var_name, tifFileName_stub, time_step):
    """
    Inputs: 
    * pointer to a netcdf file, nc
    * variable name to select from the nc
    * folder to place temporary TIFFS
    * time_step to output
    
    Outputs:
    * Formatted TIFF files ready for GEE in tmpTifFolder
    """
    
    data = nc[var_name][time_step,:,:]
    tifFile_name = tifFileName_stub + "read what this date corresponds to"
            
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

    logging.info('netCDF converted to TIFF')

    
###
## Procedure for moving tif files to the cloud
### 
    
def process_tif_files_to_cloud(tmpTifFolder, cloud_props):
    """
    Inputs:
    * folder with tif files to loop over
    * cloud_props, which at least contain keys: imageCollection, gs_bucket
    
    Outputs: files in the correct places on gs and gee
    """
    assert(type(cloud_props)==dict)
    assert(all([prop in cloud_props.keys() for prop in ["imageCollection", "gs_bucket"]]))
    
    
    tifs = glob.glob(tmpTifFolder + "/*.tif")
    for tif in tifs:
        
        # always the same for this data set
        band_names = "surface_temp_anomalies"
        
        # read time_start from the file name
        time_start = ""
        time_end = ""
        
        kwargs = {
            "tifFile_name":tif,
            "gs_bucket":cloud_props["gs_bucket"],
            "gee_props":{
                "imageCollection":cloud_props["imageCollection"],
                "gee_asset_name": "users/resourcewatch/" + cloud_props["imageCollection"] + "/" + tif,
                "band_names":band_names,
                "time_start":time_start,
                "time_end":time_end
            }
        }

        cloudProcess(**kwargs)
    
def cloudProcess(tifFile_name, gs_bucket, gee_props):
    """
    Inputs: 
    * name of the imageCollection to add to
    * name of the tifFile stored on the instance, to be uploaded
    * name of the gee_asset
    * properties to set on the gee_asset
    ** gee_props should be a dictionary w/ at least four keys: 
    ** imageCollection, gee_asset_name, band_names, time_start, time_end
    
    Outputs: files in the correct places on gs, and gee
    """
    assert(type(gee_props)==dict)
    assert(all([prop in gee_props.keys() for prop in ["imageCollection", "gee_asset_name", "band_names", "time_start", "time_end"]]))
    
    gs_loc = loadToGoogleStorage(tifFile_name, gs_bucket, gee_props["imageCollection"])
    loadToGEE(gs_loc, gee_props)

def loadToGoogleStorage(tifFile_name, gs_bucket, imageCollection):
    gs_loc = "gs://" + gs_bucket + "/raster/" + imageCollection + "/" + tifFile_name
    
    cmd = ["gsutil", "cp", tifFile_name, gs_loc]
    logging.info(subprocess.check_output(cmd))
    
    logging.info('Up on google storage')

    return(gs_loc)

def loadToGEE(gs_loc, gee_props):
    
    ### add in option to overwrite the asset if it is already up
    
    cmd = ["earthengine", "upload", "image",
    "--asset_id", gee_props["gee_asset_name"], gs_loc,
    "--pyramiding_policy=mode",
    "--bands", gee_props["band_names"],
    "-p", "system:time_start="+gee_props["time_start"],
    "-p", "system:time_end="+gee_props["time_end"]]
    logging.info(subprocess.check_output(cmd))
    
    logging.info('GEE asset upload started')
    logging.info('Check back to ensure ACL is set to public before attempting to connect to the back office')
    
    
###
## Cleaning up
###
    
def cleanUp(tmpDataFolder):
    shutil.rmtree(tmpDataFolder)
    logging.info('container process finished, container cleaned')

     
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
    # Should this return the timeframe of the collection?
    nc = download_full_nc_history(tmpNcFolder)
    var_name = 'tempanomaly'
    
    # Populate the tmpTifFolder will all files to process
    tifFileName_stub = "cli_035_surface_temp_analysis_"
    if PROCESS_HISTORY:
        process_full_history_to_tifs(nc, var_name, tmpTifFolder, tifFileName_stub)
    else:
        process_most_recent_to_tif(nc, var_name, tmpTifFolder, tifFileName_stub)
    
    # Process all files in the tmpTifFolder onto the cloud
    cloud_props = {
        "imageCollection": "cli_035_surface_temp_analysis",
        "gs_bucket": "resource-watch-public"
    }
    process_tif_files_to_cloud(tmpTifFolder, cloud_props)
    
    # Clean up before exit
    cleanUp(tmpDataFolder)

main()