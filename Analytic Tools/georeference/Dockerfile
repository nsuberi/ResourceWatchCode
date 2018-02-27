FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=georeference
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries

RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql
RUN pip install --upgrade pip && pip install \
    cartoframes \
    requests \
    pandas \
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
