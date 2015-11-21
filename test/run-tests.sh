#!/bin/bash

#set -o xtrace
set -o errexit
set -o nounset

function cleanup {
    if [ "$PROXY_PID" != "" ]; then
        kill $PROXY_PID
    fi
}

trap cleanup EXIT

mkdir -p /tmp/s3proxy

export LOG_LEVEL=warn
PROXY_BIN="java -jar s3proxy.jar --properties test/s3proxy.properties"
stdbuf -oL -eL $PROXY_BIN &
PROXY_PID=$!

go test -v ./... #-check.f TestUnlink
exit $?
