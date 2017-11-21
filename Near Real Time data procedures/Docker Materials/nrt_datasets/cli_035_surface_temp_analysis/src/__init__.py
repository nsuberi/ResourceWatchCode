import numpy as np
import os
#import urllib2
from urllib.request import urlopen
import shutil
from contextlib import closing
from netCDF4 import Dataset
import rasterio
import boto3
np.set_printoptions(threshold='nan')
s3 = boto3.resource("s3")

def dataDownload(): 
    remote_path = 'ftp://ftp.cdc.noaa.gov/Datasets/noaaglobaltemp/'
    last_file = 'air.mon.anom.nc'

    local_path = os.getcwd()

    print (remote_path)
    print (last_file)
    print (local_path)

    #Download the file .nc
    #with closing(urllib.urlopen(remote_path+last_file)) as r:
    with closing(urlopen(remote_path+last_file)) as r:
        with open(last_file, 'wb') as f:
            shutil.copyfileobj(r, f)

    ncfile = Dataset(local_path+'/'+last_file)
    
    return last_file

def netcdf2tif(dst,outFile):
    nc = Dataset(dst)
    data = nc['air'][1,:,:]
            
    data[data < -40] = -99
    data[data > 40] = -99
    #print (data)
    
    # Return lat info
    south_lat = -88.75
    north_lat = 88.75

    # Return lon info
    west_lon = -177.5
    east_lon = 177.5
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

def s3Upload(outFile, bucket, folder):
    # Push to Amazon S3 instance
    f = open(outFile,'rb')
    s3.Object(bucket, folder + outFile).put(Body=f)

# Execution
outFile ='air_temo_anomalies.tif'
print ('starting')
file = dataDownload()
print ('downloaded')
netcdf2tif(file,outFile)
print ('converted')
s3Upload(outFile, "wri-public-data", "resourcewatch/raster/cli_035_surface_temp_analysis/")
print ('finish')