from __future__ import unicode_literals

import os
import sys
import urllib.request
import shutil
from contextlib import closing
import zipfile
import datetime
import logging
import subprocess
import fiona
from collections import OrderedDict
import cartosql
from . import eeUtil

LOG_LEVEL = logging.INFO
CLEAR_TABLE_FIRST = False
VERSION = '3.0'

# Sources for average polylines
SOURCE_URL_MONTHLY_MEDIAN = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02135/{north_or_south}/monthly/shapefiles/shp_median/{target_file}'
SOURCE_FILENAME_MONTHLY_MEDIAN = 'median_extent_{N_or_S}_{month}_1981-2010_polyline_v{version}'
CARTO_TABLE = 'cli_005_polar_monthly_sea_ice_extent_polylines'
CARTO_SCHEMA = OrderedDict([
        ('the_geom', 'geometry'),
        ('date', 'text'),
        ('_uid', 'text')
    ])
UID_FIELD = '_uid'
TIME_FIELD = 'date'

# For naming and storing assets
DATA_DIR = 'data'

# Times two because of North / South parallels
DATE_FORMAT = '%Y%m'
TIMESTEP = {'days': 30}

###
## Handling VECTORS
###

def extractShp(zfile, dest):
    with zipfile.ZipFile(zfile) as z:
        shp_name = ''
        for f in z.namelist():
            if os.path.splitext(f)[1] == '.shp':
                shp_name = f
        z.extractall(dest)
    return shp_name

def genUID(arctic_or_antarctic, month, fid):
    return '_'.join([arctic_or_antarctic, month, str(fid)])

### Not needed because of ogr2ogr -wrapdateline option
# def breakGeomAt180(geom):
#     line_coords = geom['coordinates']
#     logging.debug('Number of line segments before: {}'.format(len(line_coords)))
#     new_lines = []
#     for line in line_coords:
#         lons, _ = zip(*line)
#         last_break = 0
#         for i in range(len(lons)-1):
#             lon1 = lons[i]
#             lon2 = lons[i+1]
#             if abs(lon1-lon2) > 350:
#                 new_lines.append(line[last_break:i+1])
#                 last_break=i+1
#         new_lines.append(line[last_break:])
#     geom['coordinates'] = new_lines
#     logging.debug('Number of line segments after: {}'.format(len(new_lines)))
#     return geom

def processNewVectorData(existing_ids):
    months = [str(mon) if len(str(mon))==2 else '0'+str(mon) for mon in range(1,13)]
    total_new_count = 0
    for month in months:
        for a in ['arctic', 'antarctic']:
            north_or_south = 'north' if a=='arctic' else 'south'
            filename = SOURCE_FILENAME_MONTHLY_MEDIAN.format(N_or_S=north_or_south[0].upper(), month=month, version=VERSION)
            tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,filename))

            url = SOURCE_URL_MONTHLY_MEDIAN.format(north_or_south=north_or_south, target_file='{}.zip'.format(filename))

            logging.info('Fetching {} median ice extent for {}'.format(a, month))
            logging.debug('url: {}, filename: {}'.format(url, tmpfile))
            try:
                urllib.request.urlretrieve(url, tmpfile)
                unzipped_folder = os.path.join(DATA_DIR,'unzipped_'+filename)
                shpfile = extractShp(tmpfile, unzipped_folder)

                logging.debug('shapefile name: {}'.format(shpfile))

                if a == 'arctic':
                    s_srs = 'EPSG:3411'
                else:
                    s_srs = 'EPSG:3412'

                original_shapefile = os.path.join(unzipped_folder,shpfile)
                logging.debug('Original shapefile: {}'.format(original_shapefile))
                reprojected_shapefile = os.path.join(DATA_DIR,'reprojected_'+shpfile)
                cmd = ' '.join(['ogr2ogr','-overwrite', '-f', '"ESRI Shapefile"',
                                '-wrapdateline',
                                '-s_srs',s_srs,'-t_srs','EPSG:4326',
                                reprojected_shapefile,original_shapefile,])
                subprocess.check_output(cmd, shell=True)

            except Exception as e:
                logging.warning('Could not retrieve and reproject {}'.format(url))
                logging.error(e)
                continue

            logging.info('Parsing data')

            rows = []
            with fiona.open(reprojected_shapefile, 'r') as shp:

                logging.debug(shp.schema)
                for obs in shp:

                    uid = genUID(a, month, obs['properties']['FID'])
                    if uid not in existing_ids:
                        row = []
                        for field in CARTO_SCHEMA.keys():
                            if field == 'the_geom':
                                #better_geom = breakGeomAt180(obs['geometry'])
                                row.append(obs['geometry'])
                            elif field == UID_FIELD:
                                row.append(uid)
                            elif field == TIME_FIELD:
                                row.append(month)

                        rows.append(row)

            # 3. Delete local files
            os.remove(tmpfile)

            # 4. Insert new observations
            new_count = len(rows)
            total_new_count += new_count
            if new_count:
                logging.info('Pushing new rows')
                cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                    CARTO_SCHEMA.values(), rows)
    return total_new_count

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, id_field, unique=True)
    if time_field:
        cartosql.createIndex(table, time_field)

def getIds(table, id_field):
    '''get ids from table'''
    r = cartosql.getFields(id_field, table, f='csv')
    return r.text.split('\r\n')[1:-1]


###
## Application code
###

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        cartosql.dropTable(CARTO_TABLE)

    # 1. Check collection, create table if necessary
    existing_ids = []
    if cartosql.tableExists(CARTO_TABLE):
        logging.info('Fetching existing ids')
        existing_ids = getIds(CARTO_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_TABLE))
        createTableWithIndex(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)

    # 2. Ingest new data
    num_new_vectors = processNewVectorData(existing_ids)

    # 3. Report results
    existing_count = num_new_vectors + len(existing_ids)
    logging.info('Total rows: {}, New: {}, Max: {}'.format(
        existing_count, num_new_vectors, 'none'))
