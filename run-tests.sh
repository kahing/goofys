#!/bin/bash

set -o xtrace
set -o errexit
set -o nounset

function cleanup {
    if [ "$PROXY_PID" != "" ]; then
        kill $PROXY_PID
    fi
}

trap cleanup EXIT

PROXY_BIN="java -jar s3proxy.jar --properties s3proxy.properties"
stdbuf -oL -eL $PROXY_BIN &
PROXY_PID=$!

go test -v ./... #-check.f TestUnlink
exit $?
