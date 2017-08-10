#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

: ${BUCKET:="goofys-bench"}

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

mkdir bench-mnt

S3FS="s3fs -f -ostat_cache_expire=1 -oiam_role=auto $BUCKET bench-mnt"
RIOFS="riofs -f -c $dir/riofs.conf.xml $BUCKET bench-mnt"
GOOFYS="goofys -f --stat-cache-ttl 1s --type-cache-ttl 1s --endpoint http://s3-us-west-2.amazonaws.com/ $BUCKET bench-mnt"
LOCAL="cat"

if [ ! -f ~/.passwd-riofs ]; then
    echo "RioFS password file ~/.passwd-riofs missing"
    exit 1;
fi
source ~/.passwd-riofs

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
        cat)
            FS=$LOCAL
            ;;
    esac

    rm bench.$fs 2>/dev/null || true

    if [ "$t" = "" ]; then
        for tt in create create_parallel io; do
            $dir/bench.sh "$FS" bench-mnt $tt |& tee -a bench.$fs
        done

        $dir/bench.sh "$FS"  bench-mnt ls_create

        for i in $(seq 1 10); do
            rm -Rf /tmp/riofs
            $dir/bench.sh "$FS" bench-mnt ls_ls |& tee -a bench.$fs
        done

        $dir/bench.sh "$FS" bench-mnt ls_rm
        $dir/bench.sh "$FS" bench-mnt find

    else
        $dir/bench.sh "$FS" bench-mnt $t |& tee bench.$fs
    fi
done

$dir/bench.sh cat bench-mnt $t |& tee bench.local

rmdir bench-mnt

$dir/bench_format.py <(paste $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs) > $dir/bench.data

gnuplot $dir/bench_graph.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png

for f in $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs $dir/bench.data $dir/bench.png; do
    aws s3 cp $f s3://$BUCKET/
done
