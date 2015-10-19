#!/bin/bash

set -o errexit
set -o nounset

dir=$(dirname $0)

mkdir bench-mnt
$dir/bench.sh "goofys --stat-cache-ttl 0 --type-cache-ttl 0 goofys goofys-mnt/" bench-mnt >& bench.goofys
$dir/bench.sh "s3fs -ostat_cache_expire=1 -f goofys s3fs-mnt" bench-mnt >& bench.s3fs
rmdir bench-mnt

$dir/format_bench.sh bench.s3fs bench.goofys
