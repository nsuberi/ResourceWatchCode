import logging
import sys
import eeUtil
import os
from datetime import datetime
import urllib.request

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

    folders = []
    ftp.retrlines('NLST', folders.append)
    logging.info("Folders:")
    logging.info(folders)

    data = []
    ftp.retrlines('NLST data/recent', data.append)
    data = [f.split('/')[2] for f in data]
    logging.info("Data:")
    logging.info(data)

    import re
    # Matches soil carbon for different depths:
    # 0, 5, 15, 30, 60, 100, 200 cm depth tifs available,
    # labeled sl1 - sl7
    # http://data.isric.org/geonetwork/srv/eng/catalog.search;jsessionid=A5137293CC6B3D96CBA35808CA155341#/metadata/98062ae9-911d-4e04-80a9-e4b480f87799
    pattern = re.compile('OCSTHA_M_sd._250m.tif')
    soilcarbon = [f for f in data if pattern.match(f)]
    logging.info("SoilCarbon data:")
    logging.info(soilcarbon)

    SOURCE_URL = 'ftp://ftp.soilgrids.org/data/recent/{f}'

    def getUrl(lvl):
        return SOURCE_URL.format(f=lvl)

    def getFilename(lvl):
        return 'tifs/{}'.format(lvl)

    ## Download with ftplib
    # Track progress:
    # https://stackoverflow.com/questions/21343029/how-do-i-keep-track-of-percentage-downloaded-with-ftp-retrbinary
    def download_file(f, block, totalSize):
        global sizeWritten
        f.write(block)
        sizeWritten += len(block)
        logging.info("{}= size written, {}= total size".format(sizeWritten, totalSize))
        percentComplete = sizeWritten / totalSize
        logging.info("{} percent complete".format(percentComplete))

    for data in soilcarbon:
        logging.info('Processing {}'.format(data))
        totalSize = ftp.size('data/recent/' + data)
        sizeWritten = 0
        with open('tifs/{}'.format(data), 'wb') as f:
            ftp.retrbinary('RETR data/recent/' + data, lambda block: download_file(f, block, totalSize))

    ###
    ## Download with urllib

    # def fetch(files):
    #     '''Fetch files by datestamp'''
    #     tifs = []
    #     for lvl in files:
    #         url = getUrl(lvl)
    #         f = getFilename(lvl)
    #         logging.debug('Fetching {}'.format(url))
    #         # New data may not yet be posted
    #         try:
    #             urllib.request.urlretrieve(url, f)
    #             tifs.append(f)
    #         except Exception as e:
    #             logging.warning('Could not fetch {}'.format(url))
    #             logging.debug(e)
    #     return tifs
    #
    #
    # tifs = fetch(soilcarbon)

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
            ftp.retrbinary('RETR legends/' + sld, f.write)

    ftp.close()


    ###
    # Consult Brookie on how to use his class,
    # inject the xml retrieved from FTP and attach to correct layer
    ###
