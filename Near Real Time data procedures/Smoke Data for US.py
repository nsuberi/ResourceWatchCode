
# coding: utf-8

# In[2]:

# Data handling libraries
import pandas as pd
#import fiona
#import geopandas as gpd

# Misc helpers
import json
from datetime import datetime
import os

# Authentication
from configparser import ConfigParser

config = ConfigParser()
config.read("../.env")
# FROM: https://resourcewatch.carto.com/u/wri-rw/your_apps
carto_api_token = config.get("auth", "carto_api_token")

# URL interactions
import requests as req

# Libraries for downloading data from FTP
import shutil
import urllib.request as obj_req
from contextlib import closing

remote_path = "ftp://satepsanone.nesdis.noaa.gov/FIRE/HMS/GIS/"

# data upload
import boto3
import sys
import threading

s3_upload = boto3.client("s3")
s3_download = boto3.resource("s3")
s3_bucket = "wri-public-data"
s3_folder = "resourcewatch/"

zipped_smoke = "yesterday_smoke_shapefile"
zipped_fire = "yesterday_fire_shapefile"

class ProgressPercentage(object):
        def __init__(self, filename):
            self._filename = filename
            self._size = float(os.path.getsize(filename))
            self._seen_so_far = 0
            self._lock = threading.Lock()

        def __call__(self, bytes_amount):
            # To simplify we'll assume this is hooked up
            # to a single filename.
            with self._lock:
                self._seen_so_far += bytes_amount
                percentage = (self._seen_so_far / self._size) * 100
                sys.stdout.write("\r%s  %s / %s  (%.2f%%)"%(
                        self._filename, self._seen_so_far, self._size,
                        percentage))
                sys.stdout.flush()


# In[3]:

# View smoke data files available on ftp
file = obj_req.urlopen(remote_path).read().splitlines()
file


# In[4]:

# Download most recent smoke data - or past 4 days?
# If want to include a history, need to do reformatting if 
# 4 day interval extends across 2 months

def format_month(mon):
    if mon < 10:
        return("0" + str(mon))
    else:
        return(str(mon))

now = datetime.now()
year = str(now.year)
month = format_month(now.month)
today = str(now.day)
yesterday = str(now.day-1)

def create_most_recent_file(file_type, year, month, day):
    files = [
        "hms_{}{}{}{}.prelim.shp".format(file_type, year, month, day),
        "hms_{}{}{}{}.prelim.shx".format(file_type, year, month, day),
        "hms_{}{}{}{}.prelim.dbf".format(file_type, year, month, day)
    ]
    return(files)

# recent_files = create_most_recent_file(year, month, today)
recent_smoke_files = create_most_recent_file("smoke", year, month, yesterday)
recent_fire_files = create_most_recent_file("fire", year, month, yesterday)


smoke_folder = "/Users/nathansuberi/Desktop/RW_Data/Smoke/"
yesterday_smoke_folder = "/Users/nathansuberi/Desktop/RW_Data/Smoke/yesterday_smoke/"
yesterday_fire_folder = "/Users/nathansuberi/Desktop/RW_Data/Smoke/yesterday_fire/"

for file in recent_smoke_files:
    ftp_loc = remote_path+file
    
    local_smoke = yesterday_smoke_folder + file
    print(local_smoke)
    with closing(obj_req.urlopen(ftp_loc)) as r:
        with open(local_smoke, 'wb') as f:
            shutil.copyfileobj(r, f)

for file in recent_fire_files:
    ftp_loc = remote_path+file
    local_fire = yesterday_fire_folder + file        
    print(local_fire)
    with closing(obj_req.urlopen(ftp_loc)) as r:
        with open(local_fire, 'wb') as f:
            shutil.copyfileobj(r, f)


# In[5]:

# Prepare data, upload to S3

# Zip file: https://stackoverflow.com/questions/1855095/how-to-create-a-zip-archive-of-a-directory
os.chdir(smoke_folder)
shutil.make_archive(zipped_smoke, 'zip', yesterday_smoke_folder)
shutil.make_archive(zipped_fire, 'zip', yesterday_fire_folder)

# Upload to S3
s3_upload.upload_file(zipped_smoke + ".zip", s3_bucket, s3_folder + zipped_smoke + ".zip",
                         Callback=ProgressPercentage(zipped_smoke + ".zip"))
s3_upload.upload_file(zipped_fire + ".zip", s3_bucket, s3_folder + zipped_fire + ".zip",
                         Callback=ProgressPercentage(zipped_fire + ".zip"))

# Deleteoriginal files and zipped files from local

# Unlink is the same as os.remove
for folder in [yesterday_smoke_folder, yesterday_fire_folder, smoke_folder]:
    for file in os.listdir(folder):
        file_path = os.path.join(folder, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)    


# In[6]:

# Check out which syncs are available, find the one that matches these datasets
# Fill in import_below to force the sync

res = req.get("https://wri-rw.carto.com/api/v1/synchronizations/?api_key={}".format(carto_api_token))
sync_jobs = res.json()
for pos, job in enumerate(sync_jobs["synchronizations"]):
    print("job position:", pos)
    print("job name:", job["url"])
    print("job id:", job["id"])


# In[ ]:

# Force sync, will only go through if last sync was more than 15 minutes ago
import_id = "<insert id here>"
headers = {
    "content-length":"0"
}
res = req.put("https://wri-rw.carto.com/api/v1/synchronizations/{}/sync_now?api_key={}".format(import_id, carto_api_token),
             headers=headers)
print(res.text)


# In[ ]:




# In[ ]:

### LEGACY, only need to do this once per dataset. 
# After uploading new data, update by running the force sync script above


# In[ ]:

# Sync carto table with S3 url
# ONLY NEED TO RUN THIS ONCE

smoke_data_url = "https://wri-public-data.s3.amazonaws.com/resourcewatch/" + zipped_smoke + ".zip"
fire_data_url = "https://wri-public-data.s3.amazonaws.com/resourcewatch/" + zipped_fire + ".zip"

# 3600 = sync every hour
# 3600 * 24 = sync every day
interval = str(3600*24)

smoke_payload = {
    "url":smoke_data_url,
    "interval":interval
}
fire_payload = {
    "url":fire_data_url,
    "interval":interval
}

sync_url = "https://wri-rw.carto.com/api/v1/synchronizations/?api_key={}".format(carto_api_token)
headers = {
    'content-type': "application/json"
}

smoke_res = req.request("POST", sync_url, data=json.dumps(smoke_payload), headers = headers)
print("smoke:", smoke_res.text)

fire_res = req.request("POST", sync_url, data=json.dumps(fire_payload), headers = headers)
print("fire:", fire_res.text)

