#!/bin/bash -e

go build
exec ./goofys --endpoint=http://127.0.0.1:9090 -f "$@" test mnt
