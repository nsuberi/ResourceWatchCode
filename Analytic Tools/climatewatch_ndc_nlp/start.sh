#!/bin/sh

#Change the NAME variable with the name of your script
NAME=climatewatch_ndc_nlp

docker build -t $NAME --build-arg NAME=$NAME .
docker run \
    --net="host" \
    --volume="$(pwd)"/data:/opt/$NAME/data \
    --env-file .climatewatch.env \
    --rm $NAME \
    python main.py
# --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME
