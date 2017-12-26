FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=python-script
ENV NAME ${NAME}


#### NEED TO HAVE GSUTIL, EARTHENGINE
## NEED TO AUTHENTICATE WITH EARTHENGINE
# install core libraries
RUN apt-get update
RUN apt-get install -y cron gdal-bin libgdal-dev

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Init crontab with user
RUN chmod +x cron-start.sh
CMD ["./cron-start.sh"]
