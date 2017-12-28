def deleteIndices(CARTO_TABLE):
    r = cartosql.sendSql("select * from pg_indexes where tablename='{}'".format(CARTO_TABLE))
    indexes = r.json()["rows"]
    logging.debug("Existing indices: {}".format(indexes))
    for index in indexes:
        try:
            sql = "alter table {} drop constraint {}".format(CARTO_TABLE, index["indexname"])
            r = cartosql.sendSql(sql)
            logging.debug(r.text)
        except:
            logging.error("couldn't drop constraint")
        try:
            sql = "drop index {}".format(index["indexname"])
            r = cartosql.sendSql(sql)
            logging.debug(r.text)
        except:
            logging.error("couldn't drop index")


def checkCreateTable(table, schema, id_field, time_field):
    '''
    Create table if it doesn't already exist
    '''
    if cartosql.tableExists(table):
        logging.info('Table {} already exists'.format(table))
    else:
        logging.info('Creating Table {}'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        if id_field != time_field:
            cartosql.createIndex(table, time_field)

def cleanOldRows(table, time_field, max_age, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Delete excess rows by age
    Max_Age should be a datetime object or string
    Return number of dropped rows
    '''
    num_expired = 0
    if cartosql.tableExists(table):
        if isinstance(max_age, datetime):
            max_age = max_age.strftime(date_format)
        elif isinstance(max_age, str):
            logging.error('Max age must be expressed as a datetime.datetime object')

        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_expired = r.json()['total_rows']
    else:
        logging.error("{} table does not exist yet".format(table))

    return(num_expired)

def makeRoomForNewData(table, schema, uidfield, max_rows, leftover_ids, new_ids):
    '''
    If new rows would push over limit, delete some first
    Will delete new_ids if there are too many for the table to hold
    '''
    seen_ids = leftover_ids + new_ids
    num_new_rows = len(new_ids)

    # Placeholder
    overflow_ids = []

    if len(seen_ids) > max_rows:
        if max_rows > num_new_rows:
            logging.debug("can accommodate all new_ids")
            drop_ids = leftover_ids[(max_rows - num_new_rows):]
            drop_response = cartosql.deleteRowsByIDs(table, drop_ids, id_field=uidfield, dtype=schema[uidfield])

            leftover_ids = leftover_ids[:(max_rows - num_new_rows)]
        else:
            logging.debug("cannot accommodate all new_ids")

            overflow_ids = new_ids[max_rows:]
            new_ids = new_ids[:max_rows]

            drop_ids = leftover_ids + overflow_ids
            drop_response = cartosql.deleteRowsByIDs(table, drop_ids, id_field=uidfield, dtype=schema[uidfield])

            leftover_ids = []

            num_overflow = len(overflow_ids)
            logging.warning("Drop all existing_ids, and enough oldest new ids to have MAX_ROWS number of final entries in the table.")
            logging.warning("{} new data values were lost.".format(num_overflow))

        numdropped = drop_response.json()['total_rows']
        if numdropped > 0:
            logging.info('Dropped {} old rows'.format(numdropped))

    return(leftover_ids, new_ids, overflow_ids)
