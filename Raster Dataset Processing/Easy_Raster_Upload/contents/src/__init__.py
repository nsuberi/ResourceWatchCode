import logging
import sys
import eeUtil
import os
from datetime import datetime

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    eeUtil.initJson()

    # Configure the ImageCollection you're going to add the rasters to
    GS_FOLDER = 'wat_038_modis_surface_water'
    EE_COLLECTION = 'wat_038_modis_surface_water'
    def checkCreateCollection(collection):
        '''List assests in collection else create new collection'''
        if eeUtil.exists(collection):
            return eeUtil.ls(collection)
        else:
            logging.info('{} does not exist, creating'.format(collection))
            eeUtil.createFolder(collection, True, public=True)
            return []
    existing_files = checkCreateCollection(EE_COLLECTION)

    # Make sure your data is in the rasters folder
    DATA_DIR = 'rasters'
    EXTENSIONS = ['.tif', '.nc']
    os.chdir(DATA_DIR)
    tifs = os.listdir('.') #[f for f in os.listdir('.') if os.path.splitext(f)[1] == '.tif']
    logging.info('TIFS: {}'.format(tifs))

    # Update this manually, or with a function of the tif name
    DATE_FORMAT = '%Y%j'

    def getDate(asset):
        return asset[-7:]

    dates = list(map(getDate, tifs))
    datestamps = [datetime.strptime(date, DATE_FORMAT)
                  for date in dates]

    def ic(asset):
        return '{}/{}'.format(EE_COLLECTION, os.path.splitext(asset)[0])
    asset_names = [ic(t) for t in tifs]
    eeUtil.uploadAssets(tifs, asset_names, GS_FOLDER, datestamps, public=True, timeout=30000)
