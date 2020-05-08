#!/bin/bash

docker run -d --rm --name aerospike-dev aerospike

AEROSPIKE_SERVER="$(docker inspect aerospike-dev -f '{{.NetworkSettings.Networks.bridge.IPAddress}}')"
echo "export AEROSPIKE_SERVER=${AEROSPIKE_SERVER}"
