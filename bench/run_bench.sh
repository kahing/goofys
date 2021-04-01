#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

: ${BUCKET:="goofys-bench"}
: ${GOOFYS_BUCKET:="$BUCKET"}
: ${FAST:="false"}
: ${CACHE:="false"}
: ${ENDPOINT:="http://s3-us-west-2.amazonaws.com/"}
: ${AWS_ACCESS_KEY_ID:=""}
: ${AWS_SECRET_ACCESS_KEY:=""}
: ${PROG:="s3fs"}

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

mkdir bench-mnt

S3FS_CACHE="-ouse_cache=/tmp/cache"
GCSFUSE_CACHE="" # by default performs caching
GOOFYS_CACHE="--cache /tmp/cache -o allow_other"

if [ "$CACHE" == "false" ]; then
    S3FS_CACHE=""
    GOOFYS_CACHE=""
fi

S3FS_ENDPOINT="-ourl=$ENDPOINT"
GOOFYS_ENDPOINT="--endpoint $ENDPOINT"

if test "${AWS_ACCESS_KEY_ID}" = "" && echo "${ENDPOINT}" | fgrep -q amazonaws.com; then
    S3FS_ENDPOINT="${S3FS_ENDPOINT} -oiam_role=auto"
else
    echo "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" > /etc/passwd-s3fs
    chmod 0400 /etc/passwd-s3fs
    S3FS_ENDPOINT="${S3FS_ENDPOINT} -ouse_path_request_style -osigv2"
    # s3proxy is broken https://github.com/andrewgaul/s3proxy/issues/240
    #GOOFYS_ENDPOINT="${GOOFYS_ENDPOINT} --cheap"
fi

if [ -f /usr/local/bin/blobfuse ]; then
    export AZURE_STORAGE_ACCESS_KEY=${AZURE_STORAGE_KEY}
    PROG="blobfuse"
    GOOFYS_BUCKET="wasb://${BUCKET}"
    GOOFYS_ENDPOINT=""
fi

if [ -f /root/go/bin/gcsfuse ]; then
  PROG="gcsfuse"
  GOOFYS_BUCKET="gs://${BUCKET}"
  GOOFYS_ENDPOINT=""
fi

rm -f $dir/bench.goofys $dir/bench.$PROG $dir/bench.png $dir/bench-cached.png

export BUCKET
export ENDPOINT

S3FS="s3fs -f -ostat_cache_expire=1 ${S3FS_CACHE} ${S3FS_ENDPOINT} $BUCKET bench-mnt"
GOOFYS="goofys -f --stat-cache-ttl 1s --type-cache-ttl 1s ${GOOFYS_CACHE} ${GOOFYS_ENDPOINT} ${GOOFYS_BUCKET} bench-mnt"
BLOBFUSE="blobfuse bench-mnt --container-name=$BUCKET --tmp-path=/tmp/cache"
GCSFUSE="gcsfuse --temp-dir /tmp/cache --foreground $GCSFUSE_CACHE $BUCKET bench-mnt"
LOCAL="cat"

iter=10
if [ "$FAST" != "false" ]; then
    iter=1
fi

function cleanup {
    $GOOFYS >/dev/null &
    PID=$!

    sleep 5

    for f in $dir/bench.goofys $dir/bench.$PROG $dir/bench.data $dir/bench.png $dir/bench-cached.png; do
	if [ -e $f ]; then
	    cp $f bench-mnt/
	fi
    done

    kill $PID
    fusermount -u bench-mnt || true
    sleep 1
    rmdir bench-mnt
}
trap cleanup EXIT

for fs in $PROG goofys; do
    if [ "$fs" == "-" ]; then
	continue
    fi

    if mountpoint -q bench-mnt; then
	echo "bench-mnt is still mounted"
	exit 1
    fi

    case $fs in
        s3fs)
            FS=$S3FS
            CREATE_FS=$FS
            ;;
        goofys)
            FS=$GOOFYS
            CREATE_FS=$FS
            ;;
	blobfuse)
	    FS=$BLOBFUSE
	    CREATE_FS=$FS
	    ;;
        gcsfuse)
            FS=$GCSFUSE
            CREATE_FS=$FS
            ;;
        cat)
            FS=$LOCAL
            CREATE_FS=$FS
            ;;
    esac

    if [ -e $dir/bench.$fs ]; then
	rm $dir/bench.$fs
    fi

    if [ "$t" = "" ]; then
        for tt in create create_parallel io; do
            $dir/bench.sh "$FS" bench-mnt $tt |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        done

        $dir/bench.sh "$CREATE_FS"  bench-mnt ls_create

        for i in $(seq 1 $iter); do
            $dir/bench.sh "$FS" bench-mnt ls_ls |& tee -a $dir/bench.$fs
        done

        $dir/bench.sh "$FS" bench-mnt ls_rm

        $dir/bench.sh "$CREATE_FS" bench-mnt find_create |& tee -a $dir/bench.$fs
        $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
        $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
    else
        if [ "$t" = "find" ]; then
            $dir/bench.sh "$CREATE_FS" bench-mnt find_create |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt find_find |& tee -a $dir/bench.$fs
            $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
	elif [ "$t" = "cleanup" ]; then
            $dir/bench.sh "$FS" bench-mnt cleanup |& tee -a $dir/bench.$fs
        else
            $dir/bench.sh "$FS" bench-mnt $t |& tee $dir/bench.$fs
        fi
    fi
done

$dir/bench_format.py <(paste $dir/bench.goofys $dir/bench.$PROG) > $dir/bench.data

if [ "$CACHE" = "true" ]; then
    gnuplot -c $dir/bench_graph.gnuplot $dir/bench.data $dir/bench-cached.png \
	    'goofys+catfs' $PROG && \
	convert -rotate 90 $dir/bench-cached.png $dir/bench-cached.png
else
    gnuplot -c $dir/bench_graph.gnuplot $dir/bench.data $dir/bench.png goofys $PROG \
	&& convert -rotate 90 $dir/bench.png $dir/bench.png
fi

