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

    GS_FOLDER = 'wat_038_modis_surface_water'
    EE_COLLECTION = 'wat_038_modis_surface_water'

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
    # Load file names for tifs and netcdfs
    ###

    # TIF_DATA_DIR = 'tifs'
    # os.chdir(TIF_DATA_DIR)
    # tifs = os.listdir('.') #[f for f in os.listdir('.') if os.path.splitext(f)[1] == '.tif']
    # logging.info('TIFFs: {}'.format(tifs))
    #
    # NC_DATA_DIR = 'ncs'
    # os.chdir(NC_DATA_DIR)
    # ncs = os.listdir('.') #[f for f in os.listdir('.') if os.path.splitext(f)[1] == '.tif']
    # logging.info('NetCDFs: {}'.format(ncs))


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
        with open('ncs/{}'.format(datum), 'wb') as f:
            ftp.retrbinary('RETR ' + datum, f.write)


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
        f = open(os.path.join(os.getcwd(), sld), 'wb')
        ftp.retrbinary('RETR ' + sld, f.write)

    ftp.close()

    # Q: Is this possible?
    ### reduce(lambda obj, elem: obj.append(elem),  ftp.retrlines('NLST'), [])


    ###
    # To upload to GEE, need to specify the date
    # Date formats vary by provider, some common ones include:
    ###

    ### Date encoded in asset name

    DATE_FORMAT = '%Y%j' # Year and week of year
    def getDate(asset):
        return asset[-7:]

    DATE_FORMAT = '%Y-%m-%d' # Year, month, day
    def getDate(asset):
        return asset[-10:]

    DATE_FORMAT = '%Y' # Year
    def getDate(asset):
        return asset[-4:]

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
