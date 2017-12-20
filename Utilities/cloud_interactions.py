def s3Upload(tifFile_name, bucket, folder):
    # Push to Amazon S3 instance
    f = open(tifFile_name,'rb')
    s3.Object(bucket, folder + "/" + tifFile_name).put(Body=f)
    logging.info('Up on s3')
    return("s3://"+bucket+"/"+folder+"/"+tifFile_name)

