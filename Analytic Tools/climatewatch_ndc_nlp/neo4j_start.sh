#!/bin/sh

docker run -i \
    --publish=7474:7474 --publish=7687:7687 \
    --volume=$HOME/neo4j/data:/data \
    --volume=$HOME/neo4j/logs:/logs \
    --env=NEO4J_AUTH=none \
    --env=NEO4J_dbms_allow__upgrade=true \
    neo4j:3.3
