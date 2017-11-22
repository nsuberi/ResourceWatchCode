import numpy as np
import os
#import urllib2
from urllib.request import urlopen
import shutil
from contextlib import closing
from netCDF4 import Dataset
import rasterio
import boto3
import gzip
import subprocess

np.set_printoptions(threshold='nan')
s3 = boto3.resource("s3")

def dataDownload(): 
    remote_path = 'https://data.giss.nasa.gov/pub/gistemp/'
    last_file = 'gistemp250.nc.gz'

    local_path = os.getcwd()

    print (remote_path)
    print (last_file)
    print (local_path)

    #Download the file .nc
    #with closing(urllib.urlopen(remote_path+last_file)) as r:
    with closing(urlopen(remote_path+last_file)) as r:
        with gzip.open(r, "rb") as unzipped:
            with open(last_file[:-3], 'wb') as f:
                shutil.copyfileobj(unzipped, f)
    
    return last_file[:-3]

def netcdf2tif(dst,outFile):
    nc = Dataset(dst)
    data = nc['tempanomaly'][-1,:,:]
            
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
    with rasterio.open(outFile, 'w', **profile) as dst:
        dst.write(data.astype(profile['dtype']), 1)

def cloudProcess(outfile, cloud_folder, gee_asset_name):
    cloud_key = cloud_folder + outfile
    s3Upload(outfile, "wri-public-data", cloud_key)
    print ('up on s3')
    loadToGoogleStorage(cloud_key)
    print ('up on google storage')
    loadToGEE(cloud_key, gee_asset_name, "band_info_temp")
    print ('GEE asset upload started')

def s3Upload(outFile, bucket, cloud_key):
    # Push to Amazon S3 instance
    f = open(outFile,'rb')
    s3.Object(bucket, cloud_key).put(Body=f)
def loadToGoogleStorage(cloud_key):
    cmd = ["gsutil", 
    "cp", 
    "s3://wri-public-data/" + cloud_key,
    "gs://resource-watch-public/" + cloud_key]
    print(subprocess.check_output(cmd))

def loadToGEE(cloud_key, asset_name, band_info):
    cmd = ["earthengine", "upload", "image",
    "--asset_id", asset_name,
    "gs://resource-watch-public/" + cloud_key,
    "--pyramiding_policy=mode",
    "-p", "band_names=" + band_info]
    print(subprocess.check_output(cmd))

# Execution
outFile ='air_temp_anomalies.tif'
print ('starting')
file = dataDownload()
print ('downloaded')
netcdf2tif(file,outFile)
print ('converted')
cloudProcess(outFile, 
    "resourcewatch/raster/cli_035_surface_temp_analysis/",
    "users/resourcewatch/cli_035_surface_temp_analysis")
print ('finished - check back to ensure ACL is set to public before attempting to connect to the back office')