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
if [ ! -d "data" ]; then
    mkdir ./data
    chmod -R 777 ./data
    echo "create data folder"
fi
if [ ! -d "tmp" ]; then
    mkdir ./tmp
    chmod -R 777 ./tmp
    echo "create tmp folder"
fi
if [ ! -d "config" ]; then
    mkdir ./config
    chmod -R 777 ./config
    echo "create config folder"
fi
docker-compose -f ./docker-compose.yaml up --build