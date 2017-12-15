# gdal info
#gdalinfo -stats /vsicurl/http://wri-public-data.s3.amazonaws.com/resourcewatch/raster/soc_031_new/GHS_POP_GPW41975_GLOBE_R2015A_54009_250_v1_0.tif > stats.txt
#gdalinfo -stats /vsicurl/http://wri-public-data.s3.amazonaws.com/resourcewatch/raster/soc_031_new/GHS_POP_GPW41990_GLOBE_R2015A_54009_250_v1_0.tif >> stats.txt
#gdalinfo -stats /vsicurl/http://wri-public-data.s3.amazonaws.com/resourcewatch/raster/soc_031_new/GHS_POP_GPW42000_GLOBE_R2015A_54009_250_v1_0.tif >> stats.txt
#gdalinfo -stats /vsicurl/http://wri-public-data.s3.amazonaws.com/resourcewatch/raster/soc_031_new/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.tif >> stats.txt

#echo "gdalinfo complete"




# Enter instance with: gcloud compute --project "resource-watch-gee" ssh --zone "us-east1-b" "instance-1"
# Run dockerfile with:  sudo docker run -it -u 0 -v $(pwd)/volume:/opt/python-script/data custom_gdal_python

# Move files with:
# cd "/Users/nathansuberi/Desktop/Code Portfolio/ResourceWatchCode/Near Real Time data procedures/Docker Materials/GCP_docker"
# gcloud compute scp Dockerfile instance-1:/home/nathansuberi/
# gcloud compute scp dowork.sh instance-1:/home/nathansuberi/
# gcloud compute scp .dockerignore instance-1:/home/nathansuberi/


# move the files from s3 to the instance

# display current cmd, and exit on first error
set -x -e

aws configure

# Can't run this as a script... because the aws configure will fail

aws s3 cp s3://wri-public-data/resourcewatch/raster/soc_031_new/GHS_POP_GPW41975_GLOBE_R2015A_54009_250_v1_0.tif data/
aws s3 cp s3://wri-public-data/resourcewatch/raster/soc_031_new/GHS_POP_GPW41990_GLOBE_R2015A_54009_250_v1_0.tif data/
aws s3 cp s3://wri-public-data/resourcewatch/raster/soc_031_new/GHS_POP_GPW42000_GLOBE_R2015A_54009_250_v1_0.tif data/
aws s3 cp s3://wri-public-data/resourcewatch/raster/soc_031_new/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.tif data/

echo "files moved to instance" > status.txt

# gdalwarp to vrt
gdalwarp -overwrite -t_srs epsg:4326 -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -of vrt --config GDAL_CACHEMAX 500 -wm 500 data/GHS_POP_GPW41975_GLOBE_R2015A_54009_250_v1_0.tif data/GHS_POP_GPW41975_GLOBE_R2015A_54009_250_v1_0_edit.vrt
gdalwarp -overwrite -t_srs epsg:4326 -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -of vrt --config GDAL_CACHEMAX 500 -wm 500 data/GHS_POP_GPW41990_GLOBE_R2015A_54009_250_v1_0.tif data/GHS_POP_GPW41990_GLOBE_R2015A_54009_250_v1_0_edit.vrt
gdalwarp -overwrite -t_srs epsg:4326 -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -of vrt --config GDAL_CACHEMAX 500 -wm 500 data/GHS_POP_GPW42000_GLOBE_R2015A_54009_250_v1_0.tif data/GHS_POP_GPW42000_GLOBE_R2015A_54009_250_v1_0_edit.vrt
gdalwarp -overwrite -t_srs epsg:4326 -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -of vrt --config GDAL_CACHEMAX 500 -wm 500 data/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.tif data/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0_edit.vrt

echo "gdalwarp complete" >> status.txt

# merge

gdalbuildvrt -separate -o data/soc_031_population_grid.vrt data/*_edit.vrt

echo "mergevrt complete" >> status.txt

# compress with gdal_translate
gdal_translate -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -co compress=LZW -co tiled=Yes -co BIGTIFF=YES -a_nodata -999 -of GTiff data/soc_031_population_grid.vrt data/soc_031_population_grid.tif

echo "tif merged" >> status.txt

# upload to s3
aws s3 cp data/soc_031_population_grid.tif s3://wri-public-data/resourcewatch/raster/test_docker/soc_031_population_grid.tif

aws s3 ls wri-public-data/resourcewatch/raster/test_docker/

echo "uploaded to s3" >> status.txt

#gsutil cp s3://wri-public-data/resourcewatch/raster/test_docker/soc_031_population_grid.tif gs://resource-watch-public/test_docker/soc_031_population_grid.tif

#earthengine upload image --asset_id=users/resourcewatch/test_docker_soc_031_population_grid gs://resource-watch-public/test_docker/soc_031_population_grid.tif --bands year1975,year1990,year2001

#earthengine acl set public users/resourcewatch/test_docker_soc_031_population_grid

#echo "uploaded to GEE, made public" >> status.txt
