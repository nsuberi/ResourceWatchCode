FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=soc_073_
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y \
  gdal-bin \
  libgdal-dev \
  libnetcdf-dev \
  netcdf-bin \
  && apt-get clean

RUN pip install numpy #Install this ahead of rasterio
RUN pip install --upgrade pip && pip install \
    netCDF4 \
    rasterio \
    earthengine-api \
    google-cloud-storage \
    boto3

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME /opt/$NAME/data

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME/
USER $NAME

# Launch script
CMD ["python", "main.py"]
