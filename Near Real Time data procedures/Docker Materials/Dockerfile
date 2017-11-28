FROM python:2.7
MAINTAINER Nathan <nathan.suberi@wri.org>
ARG NAME=python-script
ENV NAME ${NAME}

RUN apt-get update && \
   apt-get install -y \
   bash \
   git \
   openssl \
   build-essential \
   libffi-dev \
   libxml2 \
   libxslt-dev \
   cron

# Set user to root explicitly (would be anyway by default)
USER root


### Need to be a root to do these steps? ###
## Install netcdf tools
COPY netcdf_install/install_ntcdf4.sh /opt/install_ntcdf4.sh
RUN ls /opt
RUN /opt/install_ntcdf4.sh

# Install gdal
COPY gdal_install/before_install.sh.patch /usr/local/src/gdal-docker/
COPY gdal_install/build.sh /usr/local/src/gdal-docker/
COPY gdal_install/install.sh.patch /usr/local/src/gdal-docker/
RUN /usr/local/src/gdal-docker/build.sh

# Create non-root user to limit permissions
RUN groupadd -r $NAME && useradd -r -g $NAME $NAME

# Install pip, use to download any needed python packages...
## Why is virtualenv not included as part of requirements.txt?
RUN easy_install pip && pip install --upgrade pip
RUN pip install virtualenv

RUN mkdir -p /opt/$NAME
COPY requirements.txt /opt/$NAME/requirements.txt
RUN cd /opt/$NAME && pip install -r requirements.txt

# main.py will be different for every application
COPY main.py /opt/$NAME/main.py

# Copy the application folder inside the container
WORKDIR /opt/$NAME

COPY ./src /opt/$NAME/src
COPY ./data /opt/$NAME/data

RUN chown $NAME:$NAME /opt/$NAME
USER $NAME


# Setting volume explicitly here forces the creation of the Volume directory, regardless of whether user maps a host directory to this volume using -v HOST_DIR:/opt/$NAME/data
VOLUME /opt/$NAME/data


# Launch script
CMD ["python", "main.py"]