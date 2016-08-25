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
source ~/.passwd-riofs
mkdir bench-mnt

S3FS="s3fs -f goofys bench-mnt"
RIOFS="riofs -f -c $dir/riofs.conf.xml goofys bench-mnt"
GOOFYS="goofys -f --endpoint http://s3-us-west-2.amazonaws.com/ goofys bench-mnt"

for fs in s3fs riofs goofys; do
    case $fs in
        s3fs)
            FS=$S3FS
            ;;
        riofs)
            FS=$RIOFS
            ;;
        goofys)
            FS=$GOOFYS
            ;;
    esac

    if [ "$t" = "" ]; then
        rm bench.$fs

        for tt in create create_parallel io; do
            $dir/bench.sh "$FS" bench-mnt $tt |& tee -a bench.$fs
        done

        $dir/bench.sh "$FS"  bench-mnt ls_create

        for i in $(seq 1 10); do
            rm -Rf /tmp/riofs
            $dir/bench.sh "$FS" bench-mnt ls_ls |& tee -a bench.$fs
        done

        $dir/bench.sh "$FS" bench-mnt ls_rm

    else
        $dir/bench.sh "$FS" bench-mnt $t |& tee bench.$fs
    fi
done

$dir/bench.sh cat bench-mnt $t |& tee bench.local

rmdir bench-mnt

$dir/bench_format.py <(paste $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs) > $dir/bench.data

gnuplot $dir/bench_graph.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png
