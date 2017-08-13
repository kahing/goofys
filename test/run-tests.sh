#!/bin/bash

#set -o xtrace
set -o errexit
set -o nounset

function cleanup {
    if [ "$PROXY_PID" != "" ]; then
        kill $PROXY_PID
    fi
}

T=
if [ $# == 1 ]; then
    T="-check.f $1"
fi

trap cleanup EXIT

rm -Rf /tmp/s3proxy
mkdir -p /tmp/s3proxy

export LOG_LEVEL=warn
PROXY_BIN="java -jar s3proxy.jar --properties test/s3proxy.properties"
stdbuf -oL -eL $PROXY_BIN &
PROXY_PID=$!

go test -timeout 20m -v $(go list ./... | grep -v /vendor/) -check.vv $T
exit $?
