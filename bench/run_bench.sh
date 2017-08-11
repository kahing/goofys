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
            mkdir -p /tmp/riofs-cache
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
            $dir/bench.sh "$FS" bench-mnt $tt |& tee -a $dir/bench.$fs
        done

        $dir/bench.sh "$GOOFYS"  bench-mnt ls_create

        for i in $(seq 1 10); do
            $dir/bench.sh "$FS" bench-mnt ls_ls |& tee -a $dir/bench.$fs
        done

        $dir/bench.sh "$GOOFYS" bench-mnt ls_rm

        # riofs lies when they create files
        $dir/bench.sh "$GOOFYS" bench-mnt find_create |& tee -a $dir/bench.$fs
        $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs

    else
        if [ "$t" = "find" ]; then
            $dir/bench.sh "$GOOFYS" bench-mnt find_create |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
        else
            $dir/bench.sh "$FS" bench-mnt $t |& tee $dir/bench.$fs
        fi
    fi
done

$dir/bench.sh cat bench-mnt $t |& tee $dir/bench.local

$dir/bench_format.py <(paste $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs) > $dir/bench.data

gnuplot $dir/bench_graph.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png

$GOOFYS &
PID=$!

sleep 5

for f in $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs $dir/bench.data $dir/bench.png; do
    cp $f bench-mnt/
done

kill $PID
rmdir bench-mnt
