import os
import sys
import datetime
import logging

from netCDF4 import Dataset
import rasterio as rio
from rasterio.crs import CRS
from . import eeUtil
import boto3

LOG_LEVEL = logging.DEBUG
CLEAR_COLLECTION_FIRST = False

DATA_DIR = 'data/'
GS_PREFIX = 'soc_073_gridded_gdp'
EE_COLLECTION = 'soc_073_gridded_gdp'
FILENAME = 'soc_073_{indicator}_{resolution}_{year}'

gdp_percap_PPP = {'dataset':'GDP_per_capita_PPP_1990_2015_v2.nc',
                    'name':'gdp_percap_ppp',
                    'resolution':'5arcmin'}
gdp_PPP = {'dataset':'GDP_PPP_1990_2015_5arcmin_v2.nc',
                    'name':'gdp_ppp',
                    'resolution':'5arcmin'}
gdp_PPP_30arcsec = {'dataset':'GDP_PPP_30arcsec_v2.nc',
                    'name':'gdp_ppp',
                    'resolution':'30arcsec'}

data = [gdp_percap_PPP, gdp_PPP]
# THIS KEPT FAILING SILENTLY, NOT SURE WHY
# data = [gdp_PPP_30arcsec]

DATE_FORMAT = '%Y'

# environmental variables
with open('gcsPrivateKey.json','w') as f:
    f.write(os.getenv('GCS_JSON'))

GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS")
GEE_STAGING_BUCKET = os.environ.get("GEE_STAGING_BUCKET")
GCS_PROJECT = os.environ.get("CLOUDSDK_CORE_PROJECT")


aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
s3 = boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, imageCollection=True, public=True)
        return []

def getAssetName(indicator, resolution, year):
    '''get asset name from datestamp'''
    return os.path.join(EE_COLLECTION, FILENAME.format(indicator=indicator,resolution=resolution,year=year))

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.init(GEE_SERVICE_ACCOUNT, GOOGLE_APPLICATION_CREDENTIALS,
                GCS_PROJECT, GEE_STAGING_BUCKET)

    if CLEAR_COLLECTION_FIRST:
        eeUtil.removeAsset(EE_COLLECTION, recursive=True)

    # 1. Check if collection exists and create
    checkCreateCollection(EE_COLLECTION)

    # 2. Process data

    for ds in data:
        dataset = ds['dataset']
        name = ds['name']
        resolution = ds['resolution']

        logging.info('processing: ' + dataset)

        # Extract data
        nc = Dataset(DATA_DIR + dataset)
        var = list(nc.variables.keys())[-1]
        logging.info('variable: ' + var)
        nc_data = nc[var]

        # Set info for new tif
        nodata = nc_data.getncattr("missing_value")
        shape = nc_data.shape
        logging.info(shape)
        bands = shape[0]
        if ds == gdp_PPP_30arcsec:
            bandNames = ['1990','2000','2015']
        height = shape[1]
        width = shape[2]

        # Return lat info
        south_lat = -90
        north_lat = 90

        # Return lon info
        west_lon = -180
        east_lon = 180
        # Transformation function
        transform = rio.transform.from_bounds(west_lon, south_lat, east_lon, north_lat, width, height)

        profile = {
            'height':height,
            'width':width,
            'count':1,
            'transform':transform,
            'crs':CRS({'init': 'EPSG:4326'}),
            'driver':'GTIFF',
            'compress':'lzw',
            'dtype':'float32',
            'nodata':nodata,
            'blockxsize': 128,
            'blockysize': 128,
            'tiled': True,
            'interleave': 'band'
        }

        ds_name = os.path.splitext(dataset)[0]
        tifs = []
        dates = []
        assets = []

        for bnd in range(bands):
            yr = str(int(nc['time'][bnd]))
            logging.info(yr)

            dst_name = DATA_DIR + ds_name + '_' + yr + '.tif'
            logging.info('Destination name: ' + dst_name)

            tifs.append(dst_name)
            dates.append(yr)
            assets.append(getAssetName(name, resolution, yr))

            logging.debug('Assets: ' + str(assets))

            with rio.open(dst_name, 'w', **profile) as dst:
                logging.info("STARTING BAND")
                logging.debug(dst.profile)
                bnd_data = nc_data[bnd,:,:].astype(profile['dtype'])
                dst.write(bnd_data, indexes=1)

        # WORKAROUND FOR SILENT FAILING
        # tifs = ['data/GDP_PPP_30arcsec_v2_1990.tif', 'data/GDP_PPP_30arcsec_v2_2000.tif', 'data/GDP_PPP_30arcsec_v2_2015.tif']
        # assets = ['soc_073_gridded_gdp/soc_073_gdp_ppp_30arcsec_1990', 'soc_073_gridded_gdp/soc_073_gdp_ppp_30arcsec_2000', 'soc_073_gridded_gdp/soc_073_gdp_ppp_30arcsec_2015']
        # dates = [str(1990), str(2000), str(2015)]

        # eeUtil.uploadAssets(tifs, assets, GS_PREFIX, dates,
        #                     public=True, timeout=3000, dateformat=DATE_FORMAT)
        for ix, tif in enumerate(tifs):
            s3.upload_file(tif, 'wri-public-data', 'resourcewatch/raster/' + assets[ix] + '.tif')

    logging.info('SUCCESS')
