#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

: ${BUCKET:="goofys-bench"}
: ${FAST:="false"}
: ${CACHE:="false"}
: ${ENDPOINT:="http://s3-us-west-2.amazonaws.com/"}

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

mkdir bench-mnt

if [ ! -f ~/.passwd-riofs ]; then
    echo "RioFS password file ~/.passwd-riofs missing"
    exit 1;
fi
source ~/.passwd-riofs

S3FS_CACHE="-ouse_cache=/tmp/cache"
GOOFYS_CACHE="--cache /tmp/cache -o allow_other"

if [ "$CACHE" == "false" ]; then
    S3FS_CACHE=""
    GOOFYS_CACHE=""
fi

S3FS_ENDPOINT="-ourl=$ENDPOINT"
GOOFYS_ENDPOINT="--endpoint $ENDPOINT"

if echo "${ENDPOINT}" | fgrep -q amazonaws.com; then
    S3FS_ENDPOINT="${S3FS_ENDPOINT} -oiam_role=auto"
else
    echo "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" > /etc/passwd-s3fs
    chmod 0400 /etc/passwd-s3fs
    S3FS_ENDPOINT="${S3FS_ENDPOINT} -ouse_path_request_style -osigv2"
    # s3proxy is broken https://github.com/andrewgaul/s3proxy/issues/240
    GOOFYS_ENDPOINT="${GOOFYS_ENDPOINT} --cheap"
fi

export BUCKET
export ENDPOINT
perl -p -i -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' $dir/riofs.conf.xml

S3FS="s3fs -f -ostat_cache_expire=1 ${S3FS_CACHE} ${S3FS_ENDPOINT} $BUCKET bench-mnt"
RIOFS="riofs -f -c $dir/riofs.conf.xml $BUCKET bench-mnt"
GOOFYS="goofys -f --stat-cache-ttl 1s --type-cache-ttl 1s ${GOOFYS_CACHE} ${GOOFYS_ENDPOINT} $BUCKET bench-mnt"
LOCAL="cat"

iter=10
if [ "$FAST" != "false" ]; then
    iter=1
fi

for fs in riofs s3fs goofys; do
    case $fs in
        s3fs)
            FS=$S3FS
            CREATE_FS=$FS
            ;;
        riofs)
            FS=$RIOFS
            # riofs lies when they create files
            CREATE_FS=$GOOFYS
            ;;
        goofys)
            FS=$GOOFYS
            CREATE_FS=$FS
            ;;
        cat)
            FS=$LOCAL
            CREATE_FS=$FS
            ;;
    esac
    

    rm $dir/bench.$fs 2>/dev/null || true

    if [ "$t" = "" ]; then
        if [ "$CACHE" = "true" ]; then
            $dir/bench.sh "$FS" bench-mnt io |& tee -a $dir/bench.$fs
            $dir/bench.sh "$GOOFYS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        else
            for tt in create create_parallel io; do
                $dir/bench.sh "$FS" bench-mnt $tt |& tee -a $dir/bench.$fs
                $dir/bench.sh "$GOOFYS" bench-mnt cleanup |& tee -a $dir/bench.$fs
            done

            $dir/bench.sh "$CREATE_FS"  bench-mnt ls_create

            for i in $(seq 1 $iter); do
                $dir/bench.sh "$FS" bench-mnt ls_ls |& tee -a $dir/bench.$fs
            done

            $dir/bench.sh "$GOOFYS" bench-mnt ls_rm

            $dir/bench.sh "$CREATE_FS" bench-mnt find_create |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
            $dir/bench.sh "$GOOFYS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        fi

    else
        if [ "$t" = "find" ]; then
            $dir/bench.sh "$CREATE_FS" bench-mnt find_create |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
            $dir/bench.sh "$GOOFYS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        else
            $dir/bench.sh "$FS" bench-mnt $t |& tee $dir/bench.$fs
        fi
    fi
done

$dir/bench.sh cat bench-mnt $t |& tee $dir/bench.local

$dir/bench_format.py <(paste $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs) > $dir/bench.data

if [ "$CACHE" = "true" ]; then
    gnuplot $dir/bench_graph_cached.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png
else
    gnuplot $dir/bench_graph.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png
fi

$GOOFYS >/dev/null &
PID=$!

sleep 5

for f in $dir/bench.goofys $dir/bench.s3fs $dir/bench.riofs $dir/bench.data $dir/bench.png; do
    cp $f bench-mnt/
done

kill $PID
fusermount -u bench-mnt || true
sleep 1
rmdir bench-mnt
