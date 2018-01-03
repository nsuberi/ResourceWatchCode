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
