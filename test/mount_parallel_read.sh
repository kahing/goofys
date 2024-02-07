#!/bin/bash -e

echo 'building ...'
go build

echo 'Starting the server ...'
exec ./goofys --endpoint=http://127.0.0.1:9090 -f "$@" test mnt
