FROM geodata/gdal
MAINTAINER Nathan <nathan.suberi@wri.org>

ENV PYTHON_VERSION 2.7.11
ENV PYTHON_PIP_VERSION 8.0.2
ENV NUM_CORES 4

RUN locale-gen en_US.UTF-8  
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8 

RUN apt-get -qq remove ffmpeg
# remove several traces of python
RUN apt-get purge -y python.*

# Add the following two dependencies if you want to use --enable-gnutls in FFPMEG: gnutls-bin
RUN echo deb http://archive.ubuntu.com/ubuntu trusty universe multiverse >> /etc/apt/sources.list; \
    apt-get update -qq && apt-get install -y --force-yes \
    ant \
    autoconf \
    automake \
    build-essential \
    curl \
    checkinstall \
    cmake \
    default-jdk \
    f2c \
    gfortran \
    git \
    g++ \
    imagemagick \
    libass-dev \
    libatlas-base-dev \
    libavcodec-dev \
    libavformat-dev \
    libcnf-dev \
    libfaac-dev \
    libfreeimage-dev \
    libjpeg-dev \
    libjasper-dev \
    libgnutls-dev \
    liblapack3 \
    libmp3lame-dev \
    libpq-dev \
    libpng-dev \
    libssl-dev \
    libtheora-dev \
    libtiff4-dev \
    libtool \
    libxine-dev \
    libxvidcore-dev \
    libv4l-dev \
    libvorbis-dev \
    mercurial \
    openssl \
    pkg-config \
    postgresql-client \
    supervisor \
    wget \
    unzip; \
    apt-get clean

# gpg: key 18ADD4FF: public key "Benjamin Peterson <benjamin@python.org>" imported
ENV GPG_KEY C01E1CAD5EA2C4F0B8E3571504C367C218ADD4FF

RUN set -ex \
	&& gpg --keyserver ha.pool.sks-keyservers.net --recv-keys "$GPG_KEY" \
	&& curl -fSL "https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz" -o python.tar.xz \
	&& curl -fSL "https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz.asc" -o python.tar.xz.asc \
	&& gpg --verify python.tar.xz.asc \
	&& mkdir -p /usr/src/python \
	&& tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz \
	&& rm python.tar.xz* \
	&& cd /usr/src/python \
	&& ./configure --enable-shared --enable-unicode=ucs4 \
	&& make -j$(nproc) \
	&& make install \
	&& ldconfig \
	&& curl -fSL 'https://bootstrap.pypa.io/get-pip.py' | python2 \
	&& pip install --no-cache-dir --upgrade pip==$PYTHON_PIP_VERSION \
	&& find /usr/local \
		\( -type d -a -name test -o -name tests \) \
		-o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
		-exec rm -rf '{}' + \
	&& rm -rf /usr/src/python

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

# Create non-root user to limit permissions
RUN groupadd -r $NAME && useradd -r -g $NAME $NAME

# Install pip, use to download any needed python packages...
## Why is virtualenv not included as part of requirements.txt?
RUN easy_install pip && pip install --upgrade pip

# Use pip to install some key files
RUN pip install virtualenv
RUN pip install awscli
RUN pip install google-api-python-client
RUN pip install earthengine-api

#RUN mkdir -p /opt/$NAME
#COPY requirements.txt /opt/$NAME/requirements.txt
#RUN cd /opt/$NAME && pip install -r requirements.txt



# Create an environment variable for the correct distribution
#RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"

# Add the Cloud SDK distribution URI as a package source
#RUN echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud Platform public key
#RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

# Update the package list and install the Cloud SDK
#RUN sudo apt-get update && sudo apt-get install google-cloud-sdk





# main.py launches the dataset specific code, contained in the /src directory
### Why not have the code directly in main.py??
COPY dowork.sh /opt/$NAME/dowork.sh

WORKDIR /opt/$NAME



# Assign correct permissions to user/group to modify files in /opt/$NAME
RUN chown $NAME:$NAME /opt/$NAME
# Set user with limited permissions
USER $NAME

# Setting volume explicitly here forces the creation of the Volume directory, regardless of whether user maps a host directory to this volume using -v HOST_DIR:/opt/$NAME/data
VOLUME /opt/$NAME/data

# Launch script
CMD ["/bin/bash", "dowork.sh"]