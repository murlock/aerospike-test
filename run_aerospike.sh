#!/bin/bash

docker run -d --rm --name aerospike-dev aerospike

MONGO_SERVER="$(docker inspect aerospike-dev -f '{{.NetworkSettings.Networks.bridge.IPAddress}}')"
echo "export MONGO_SERVER=${MONGO_SERVER}"
