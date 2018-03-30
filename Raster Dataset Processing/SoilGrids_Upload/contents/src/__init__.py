import logging
import sys
import eeUtil
import os
from datetime import datetime

def main():

    ###
    # Configure logging
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    # Authenticate to GEE
    eeUtil.initJson()
    ###

    ###
    # Configure the ImageCollection you're going to add the rasters to
    ###

    GS_FOLDER = 'foo_054_soil_organic_carbon'
    EE_COLLECTION = 'foo_054_soil_organic_carbon'

    def ic(asset):
        return '{}/{}'.format(EE_COLLECTION, os.path.splitext(asset)[0])

    def checkCreateCollection(collection):
        '''List assests in collection else create new collection'''
        if eeUtil.exists(collection):
            return eeUtil.ls(collection)
        else:
            logging.info('{} does not exist, creating'.format(collection))
            eeUtil.createFolder(collection, True, public=True)
            return []

    existing_files = checkCreateCollection(EE_COLLECTION)



    ###
    # Obtain names of files to upload
    ###

###
# Priority 1: Load files to GEE and register w/ RW API
###

    from ftplib import FTP
    ftp = FTP('ftp.soilgrids.org')
    ftp.login()

    lines = []
    ftp.retrlines('NLST', lines.append)


    data = []
    ftp.retrlines('NLST data/recent', data.append)
    data = [f.split('/')[2] for f in data]
    logging.info("Data:")
    logging.info(data)

    import re

    pattern = re.compile('OCDENS_M_sl._250m.tif')
    soilcarbon = [f for f in data if pattern.match(f)]
    logging.info("SoilCarbon data:")
    logging.info(soilcarbon)

    #for datum in data:
    for datum in soilcarbon:
        logging.info('Processing {}'.format(datum))
        with open('tifs/{}'.format(datum), 'wb') as f:
            ftp.retrbinary('RETR ' + datum, f.write)

    TIF_DATA_DIR = 'tifs'
    os.chdir(TIF_DATA_DIR)
    tifs = [f for f in os.listdir('.') if os.path.splitext(f)[1] == '.tif']
    logging.info('TIFFs: {}'.format(tifs))

    ###
    # To upload to GEE, need to specify the date
    # Date formats vary by provider, some common ones include:
    ###
    ### Constant year

    DATE_FORMAT = '%Y' # Year
    def getDate(asset):
        return '2017'

    ### Grab dates, create datestamps, upload through GEE

    dates = list(map(getDate, tifs))
    datestamps = [datetime.strptime(date, DATE_FORMAT)
                  for date in dates]

    asset_names = [ic(t) for t in tifs]
    eeUtil.uploadAssets(tifs, asset_names, GS_FOLDER, datestamps, public=True, timeout=30000)



    ###
    # Upload to RW API
    # For this and writing in the SLDs, could use Brookie's class
    # Would match the SLD name to the tif name, pair them and upload (like a zip)
    ###

    API_TOKEN = os.environ.get('rw_api_token', None)

    def createHeaders():
        return {
            'content-type': "application/json",
            'authorization': "Bearer {}".format( AUTH_TOKEN )
        }

    def upload_ic_to_backoffice(wri_id, imageCollectionName, datasetName):

        ds_specs = {
            "connectorType":"rest",
            "provider":"gee",
            "tableName":imageCollectionName,
            "application":["rw"],
            "geoInfo":True,
            "type":"raster",
            "name":"{}_{}".format(wri_id, datasetName)
        }

        create_res = req.request("POST",
                          'https://staging-api.globalforestwatch.org/v1/dataset',
                          data=json.dumps(ds_specs),
                          headers = createHeaders())

        logging.info(create_res.text)

        return create_res.json()['data']['id']

    rw_id = upload_ic_to_backoffice('foo.054', EE_COLLECTION, 'Soil Organic Carbon')


###
# Priority 2: Access pre-made SLDs for loading to layers ###
###

    ###
    # Retrieving legends for upload to RW API
    ###

    legends = []
    ftp.retrlines('NLST legends', legends.append)
    slds = [f.split('/')[1] for f in legends if os.path.splitext(f)[1] == '.sld']

    for sld in slds:
        logging.info('Processing {}'.format(sld))
        with open('slds/{}'.format(sld), 'wb') as f:
            ftp.retrbinary('RETR ' + sld, f.write)

    ftp.close()


    ###
    # Consult Brookie on how to use his class,
    # inject the xml retrieved from FTP and attach to correct layer
    ###

    
