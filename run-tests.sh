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

function wait_for_proxy
{

    for i in $(seq 30);
    do
        if exec 3<>"/dev/tcp/localhost/8080";
        then
            exec 3<&-  # Close for read
            exec 3>&-  # Close for write
            return
        fi
        sleep 1
    done

    # we didn't start correctly
    exit
}

PROXY_BIN="java -jar s3proxy.jar --properties s3proxy.properties"
stdbuf -oL -eL $PROXY_BIN &
PROXY_PID=$!

wait_for_proxy

go test -v ./... #-check.f TestUnlink
exit $?
