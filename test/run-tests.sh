#!/bin/bash

#set -o xtrace
set -o errexit
set -o nounset

: ${CLOUD:="s3"}
: ${PROXY_BIN:=""}
: ${PROXY_PID:=""}
: ${TIMEOUT:="10m"}
: ${MOUNT:="false"}
: ${GOOGLE_APPLICATION_CREDENTIALS:=""}

export MOUNT

function cleanup {
    if [ "$PROXY_PID" != "" ]; then
        kill $PROXY_PID
    fi
}

T=
if [ $# == 1 ]; then
    T="-check.f $1"
elif [ "$MOUNT" != "false" ]; then
    T="-check.f GoofysTest.TestMount$"
fi

trap cleanup EXIT

if [ $CLOUD == "s3" ]; then
    mkdir -p /tmp/s3proxy

    AWS_PROFILE=${AWS:-}
    if [ "$AWS_PROFILE" == "" ]; then
	: ${LOG_LEVEL:="warn"}
	export LOG_LEVEL
	if [ "$MOUNT" != "false" ]; then
	    PROXY_BIN="java -jar s3proxy.jar --properties test/s3proxy-fs.properties"
	else
	    PROXY_BIN="java -Xmx4g -jar s3proxy.jar --properties test/s3proxy.properties"
	fi
    else
	export AWS
    fi
elif [ $CLOUD == "azblob" ]; then
    : ${AZURE_STORAGE_ACCOUNT:="devstoreaccount1"}
    : ${AZURE_STORAGE_KEY:="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="}
    : ${ENDPOINT:="http://127.0.0.1:8080/${AZURE_STORAGE_ACCOUNT}/"}

    if [ ${AZURE_STORAGE_ACCOUNT} == "devstoreaccount1" ]; then
	if ! which azurite >/dev/null; then
	    echo "Azurite missing, run:" >&1
	    echo "npm install -g azurite" >&1
	    exit 1
	fi

	rm -Rf /tmp/azblob
	mkdir -p /tmp/azblob
	PROXY_BIN="azurite-blob -l /tmp/azblob --blobPort 8080 -s"
	#PROXY_BIN="azurite-blob -l /tmp/azblob --blobPort 8080 -d /dev/stdout"
    fi

    export AZURE_STORAGE_ACCOUNT
    export AZURE_STORAGE_KEY
    export ENDPOINT
elif [ $CLOUD == "adlv1" ]; then
    TIMEOUT=40m
elif [ $CLOUD == "gcs" ]; then
    if [ "$GOOGLE_APPLICATION_CREDENTIALS" = "" ]; then
      echo "GOOGLE_APPLICATION_CREDENTIALS must be set" 1>&2
      exit 1
    fi
fi

if [ "$PROXY_BIN" != "" ]; then
    stdbuf -oL -eL $PROXY_BIN &
    PROXY_PID=$!
elif [ "$TIMEOUT" == "10m" ]; then
    # higher timeout for testing to real cloud
    TIMEOUT=45m
fi

export CLOUD

# run test in `go test` local mode so streaming output works
(cd internal; go test -v -timeout $TIMEOUT -check.vv $T)
exit $?
