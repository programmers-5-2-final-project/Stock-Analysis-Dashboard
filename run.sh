#!/bin/sh
if [ ! -d "logs" ]; then
    mkdir ./logs
    chmod -R 777 ./logs
    echo "create logs folder"
fi
if [ ! -d "plugins" ]; then
    mkdir ./plugins
    chmod -R 777 ./plugins
    echo "create plugins folder"
fi
if [ ! -d "dags" ]; then
    mkdir ./dags
    chmod -R 777 ./dags
    echo "create dags folder"
fi
docker-compose -f ./docker-compose.yaml up --build