#!/bin/sh

#Change the NAME variable with the name of your script
NAME=climatewatch_ndc_nlp

docker run -id \
    --publish=7474:7474 --publish=7687:7687 \
    --volume=$HOME/neo4j/data:/data \
    --volume=$HOME/neo4j/logs:/logs \
    --env=NEO4J_AUTH=none \
    neo4j:3.3.3

docker build -t $NAME --build-arg NAME=$NAME .
docker run \
    --volume="$(pwd)"/data:/opt/$NAME/data \
    --env-file .climatewatch.env \
    --rm $NAME \
    python main.py
# --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME