#!/bin/bash

set -o errexit
set -o nounset

dir=$(dirname $0)

mkdir bench-mnt
$dir/bench.sh cat bench-mnt |& tee bench.local
$dir/bench.sh "goofys --stat-cache-ttl 0 --type-cache-ttl 0 goofys bench-mnt" bench-mnt |& tee bench.goofys
$dir/bench.sh "s3fs -ostat_cache_expire=1 -ourl=https://s3.amazonaws.com -f goofys bench-mnt" bench-mnt |& tee bench.s3fs
$dir/bench.sh "riofs -f -c riofs.conf.xml goofys bench-mnt" bench-mnt |& tee bench.riofs
rmdir bench-mnt

$dir/format_bench.sh bench.s3fs bench.goofys
$dir/format_bench.sh bench.s3fs bench.local
