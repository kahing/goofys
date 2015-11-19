#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

mkdir bench-mnt
$dir/bench.sh cat bench-mnt $t |& tee bench.local
$dir/bench.sh "goofys -f --stat-cache-ttl 0 --type-cache-ttl 0 goofys bench-mnt" bench-mnt $t |& tee bench.goofys
$dir/bench.sh "s3fs -ostat_cache_expire=1 -ourl=https://s3.amazonaws.com -f goofys bench-mnt" bench-mnt $t |& tee bench.s3fs
$dir/bench.sh "riofs -f -c riofs.conf.xml goofys bench-mnt" bench-mnt $t |& tee bench.riofs
rmdir bench-mnt

$dir/bench_format.py <(paste bench.goofys bench.s3fs bench.riofs) > $dir/bench.data

gnuplot $dir/bench_graph.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png
