FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=python-script
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron gdal-bin libgdal-dev

# install python libraries
RUN pip install requests fiona numpy rasterio

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME && cd /opt/$NAME
COPY main.py /opt/$NAME/
COPY ./src /opt/$NAME/src
COPY ./data /opt/$NAME/data
COPY time.cron /opt/$NAME/

# Add user
RUN addgroup $NAME && adduser --disabled-password --ingroup $NAME $NAME
RUN chown $NAME:$NAME /opt/$NAME

WORKDIR /opt/$NAME
VOLUME /opt/$NAME/data

# Add job to crontab
RUN echo "$(cat time.cron) cd /opt/$NAME/ && python main.py" > cron.tmp && \
    crontab -u $NAME cron.tmp && \
    rm cron.tmp

# Launch cron
CMD ["crond", "-f", "-d", "2"]
