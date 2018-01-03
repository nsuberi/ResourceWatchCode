import boto3
import io
import pandas as pd

### Functions for reading and uploading data to/from S3

s3_bucket = "wri-public-data"
ACCESS_KEY = os.environ.get('aws_access_key_id')
SECRET_KEY = os.environ.get('aws_secret_access_key')

s3_bucket = "wri-public-data"
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
    )

s3_resource = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
    )

def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8")
    return(df)

def write_to_S3(df, key, bucket=s3_bucket):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())



def cloudProcess(localTif_loc, gs_loc, gee_props):
    """
    Inputs:
    * location of the tif to upload
    * loc to upload to on google storage
    * properties to set on the gee_asset, gee_props
    ** gee_props should be a dictionary w/ at least six keys:
    ** imageCollection, gee_asset_name, band_names, nodata_value, time_start, time_end

    Outputs: files in the correct places on gs, and gee

    Assumes: Collection of correct name already exists on GEE
    """
    assert(type(gee_props)==dict)
    assert(all([prop in gee_props.keys() for prop in ["imageCollection", "gee_asset_name", "band_names", "nodata_value", "time_start", "time_end"]]))

    loadToGoogleStorage(localTif_loc, gs_loc)
    loadToGEE(gs_loc, gee_props)

def loadToGoogleStorage(localTif_loc, gs_loc):
    cmd = ["gsutil", "cp", localTif_loc, gs_loc]
    logging.info(subprocess.check_output(cmd))
    logging.info(localTif_loc.split('/')[-1] + ' up on google storage')

def loadToGEE(gs_loc, gee_props):

    # Do I need to include the CRS with --crs CRS?
    cmd = ["earthengine", "upload", "image", "--force",
    "--asset_id", gee_props["gee_asset_name"], gs_loc,
    "--nodata_value", gee_props["nodata_value"],
    "--pyramiding_policy=mode",
    "--bands", gee_props["band_names"],
    "-p", "system:time_start="+gee_props["time_start"],
    "-p", "system:time_end="+gee_props["time_end"]]
    try:
        logging.info(subprocess.check_output(cmd))
        logging.info('GEE asset upload started for ' + gee_props["gee_asset_name"])
        logging.info('Check back to ensure ACL is set to public before attempting to connect to the back office')

    except:
        logging.error(gs_loc)
        logging.error(gee_props)
        logging.error("Unexpected error:" + str(sys.exc_info()[0]))
