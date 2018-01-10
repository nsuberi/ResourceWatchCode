def insertIfNew(newUID, newValues, existing_ids, new_data):
    '''
    For new UID, values, check whether this is already in our table
    If not, add it
    Return new_ids and new_data
    '''
    seen_ids = existing_ids + list(new_data.keys())
    if newUID not in seen_ids:
        new_data[newUID] = newValues
        logging.debug("Adding {} data to table".format(newUID))
    else:
        logging.debug("{} data already in table".format(newUID))
    return(new_data)

def cleanUp(tmpDataFolder):
    shutil.rmtree(tmpDataFolder)


def process_tif_files_to_cloud(tmpTifFolder, cloud_props, asset_props):
    """
    Inputs:
    * folder with tif files to loop over
    * cloud_props, which at least contain keys: imageCollection, gs_bucket
    * asset_props, which at least contain keys: nodata_val, band_names
    Outputs: files in the correct places on gs and gee
    """
    assert(type(cloud_props)==dict)
    assert(all([prop in cloud_props.keys() for prop in ["imageCollection", "gs_bucket"]]))

    # Create collection if doesn't already exist
    cmd = ["earthengine", "create", "collection",
           "users/resourcewatch/" + cloud_props["imageCollection"]]
    logging.info(subprocess.check_output(cmd))

    tifs = glob.glob(tmpTifFolder + "*_.tif")

    for ix, tif in enumerate(tifs):

        # read time_start from the file name
        # [-15:-5] is derived from the file name convention
        if ix < (len(tifs)-1):
            time_start = tif[-15:-5]
            time_end = tifs[ix+1][-15:-5]
        else:
            time_start = tif[-15:-5]
            ### TO DO
            ## Adjust this... should just increment the month by 1, accounting for year overflow
            ##
            time_end = (datetime.datetime.strptime(time_start,"%Y-%m-%d") + datetime.timedelta(days=31)).strftime("%Y-%m-%d")

        # Times need to be expressed in milliseconds since last epoch, UNIX time
        # https://en.wikipedia.org/wiki/Unix_time
        # https://developers.google.com/earth-engine/glossary
        # See discussion here: https://groups.google.com/forum/#!searchin/google-earth-engine-developers/Value$20for$20property$20$27system$3Atime_start$27$20must$20be$20a$20number.%7Csort:date/google-earth-engine-developers/OG-G_7JzQGA/rnf-9oOIGwAJ
        # Livia.p's comment on 11/7/16
        time_start, time_end = cloud.format_time_for_gee(time_start, time_end)

        ## Some name formatting issues:
        # This isolates the tif name
        tifFile_name = tif.split("/")[-1]
        # The [:-4] below strips .tif from the asset name
        assetName = tifFile_name[:-5]
        kwargs = {
            "localTif_loc":tif,
            "gs_loc":"gs://"+cloud_props["gs_bucket"]+"/raster/"+cloud_props["imageCollection"]+"/"+tifFile_name,
            "gee_props":{
                "imageCollection":cloud_props["imageCollection"],
                "gee_asset_name": "users/resourcewatch/" + cloud_props["imageCollection"] + "/" + assetName,
                "band_names":asset_props["band_names"],
                "nodata_value":asset_props["nodata_val"],
                "time_start":str(int(time_start)),
                "time_end":str(int(time_end))
            }
        }

        cloud.cloudProcess(**kwargs)
        
def process_full_history_to_tifs(nc, time_var_name, data_var_name,
                                 tmpTifFolder, tifFileName_stub):
    # Create reference time, and list of time displacements
    ref_time, time_displacements = prepare_time_displacements(nc, time_var_name)

    # Create dates ready for tif names
    formatted_dates = misc.create_formatted_dates(ref_time, time_displacements)

    # Convert nc to tifs
    netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_dates, "full_history")

def process_partial_history_to_tifs(nc, time_var_name, data_var_name,
                                 tmpTifFolder, tifFileName_stub, num_to_keep):
    # Create reference time, and list of time displacements
    ref_time, time_displacements = prepare_time_displacements(nc, time_var_name)

    # Create dates ready for tif names
    formatted_dates = misc.create_formatted_dates(ref_time, time_displacements[-num_to_keep:])

    # Convert nc to tifs
    netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_dates, "partial_history")

def process_most_recent_to_tif(nc, time_var_name, data_var_name,
                               tmpTifFolder, tifFileName_stub):
    ### TO DO
    ## Check to see if this is a new addition
    ## For now, simply overwrite
    ###

    # Create reference time, and list of time displacements
    ref_time, time_displacements = prepare_time_displacements(nc, time_var_name)

    # Create date ready for tif name
    formatted_date = misc.create_formatted_dates(ref_time, [time_displacement[-1]])

    # Convert nc to tif
    netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_date, "most_recent")

def netcdf2tif(nc, data_var_name, tmpTifFolder, tifFileName_stub, formatted_datez, type_run):
    """
    Inputs:
    * pointer to a netcdf file, nc
    * variable name to select from the nc
    * folder to place temporary TIFFS
    * base for the tif file names
    * list of formatted_datez corresponding to entries in the nc file... must be a list

    Outputs:
    * Formatted TIFF files ready for GEE in tmpTifFolder
    """
    assert(type(formatted_datez)==list)
    assert(type_run in ["full_history", "partial_history", "most_recent"])

    for time_step, date in enumerate(formatted_datez):
        # Intercept the case where we're only looking at the most recent observation,
        # not the entire history

        if type_run == "full_history":
            time_step=time_step
        elif type_run == "partial_history":
            time_step=-(len(formatted_datez)+time_step)
        else:
            time_step = -1

        data = nc[data_var_name][time_step,:,:]
        tifFile_name = tmpTifFolder + tifFileName_stub + date + "_.tif"

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

        logging.info('netCDF converted to TIFF' + tifFile_name)
