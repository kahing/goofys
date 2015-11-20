#!/bin/bash

: ${TRAVIS:="false"}

iter=10

if [ "$TRAVIS" != "false" ]; then
    set -o xtrace
    iter=3
fi

set -o errexit
set -o nounset

if [ $# -lt 2 ]; then
    echo "Usage: $0 <mount cmd> <dir>"
    exit 1
fi

cmd=$1
mnt=$2
if [ $# -gt 2 ]; then
    t=$3
else
    t=
fi

prefix=$mnt/test_dir

$cmd >& mount.log &
PID=$!

function cleanup {
    popd >/dev/null
    rmdir $prefix >& /dev/null || true # riofs doesn't support rmdir

    if [ "$PID" != "" ]; then
        kill $PID >& /dev/null || true
        fusermount -u $mnt >& /dev/null || true
    fi
}

function cleanup_err {
    err=$?
    popd >&/dev/null || true
    rmdir $prefix >&/dev/null || true

    if [ "$PID" != "" ]; then
        kill $PID >& /dev/null || true
        fusermount -u $mnt >& /dev/null || true
    fi

    return $err
}

trap cleanup EXIT
trap cleanup_err ERR

if [ "$TRAVIS" == "false" ]; then
    sleep 5
fi
mkdir "$prefix"
pushd "$prefix" >/dev/null

function drop_cache {
    if [ "$TRAVIS" == "false" ]; then
        (echo 3 | sudo tee /proc/sys/vm/drop_caches) > /dev/null
    fi
}

export TIMEFORMAT=%R

function run_test {
    test=$1
    drop_cache
    sleep 1
    echo -n "$test "
    time $test
}

function get_howmany {
    if [ "$TRAVIS" != "false" ]; then
        howmany=10
    else
        if [ $# == 0 ]; then
            howmany=100
        else
            howmany=$1
        fi
    fi
}

function create_files {
    get_howmany $@

    for i in $(seq 1 $howmany); do
        echo $i > file$i
    done
}

function ls_files {
    # people usually use ls in the terminal when color is on
    ls --color=always > /dev/null
}

function rm_files {
    get_howmany $@

    for i in $(seq 1 $howmany); do
        rm file$i >&/dev/null || true
    done
}

function create_files_parallel {
    get_howmany $@

    (for i in $(seq 1 $howmany); do
        echo $i > file$i & true
    done
    wait)
}

function rm_files_parallel {
    get_howmany $@

    (for i in $(seq 1 $howmany); do
        rm file$i & true
    done
    wait)
}

function write_large_file {
    dd if=/dev/zero of=largefile bs=1MB count=1000 status=none
}

function read_large_file {
    dd if=largefile of=/dev/null bs=1MB status=none
}

function read_first_byte {
    dd if=largefile of=/dev/null bs=1 count=1 status=none
}

if [ "$t" = "" -o "$t" = "create" ]; then
    for i in $(seq 1 $iter); do
        run_test create_files
        run_test rm_files
    done
fi

if [ "$t" = "" -o "$t" = "create_parallel" ]; then
    for i in $(seq 1 $iter); do
        run_test create_files_parallel
        run_test rm_files_parallel
    done
fi

if [ "$t" = "" -o "$t" = "ls" ]; then
    create_files_parallel 1000
    for i in $(seq 1 $iter); do
        run_test ls_files
    done
    rm_files 1000
fi

if [ "$t" = "" -o "$t" = "io" ]; then
    for i in $(seq 1 $iter); do
        run_test write_large_file
        run_test read_large_file
        run_test read_first_byte
        rm largefile
    done
fi

function write_md5 {
    seed=$(dd if=/dev/urandom bs=128 count=1 status=none | base64 -w 0)
    random_cmd="openssl enc -aes-256-ctr -pass pass:$seed -nosalt"
    MD5=$(dd if=/dev/zero bs=1MB count=1000 status=none | $random_cmd | tee >(md5sum) >largefile | cut -f 1 '-d ')
}

function read_md5 {
    READ_MD5=$(md5sum largefile | cut -f 1 '-d ')
    if [ "$READ_MD5" != "$MD5" ]; then
        echo "$READ_MD5 != $MD5" >&2
        rm largefile
        exit 1
    fi
}

if [ "$t" = "" -o "$t" = "md5" ]; then
    write_md5
    for i in $(seq 1 $iter); do
        run_test read_md5
    done
    rm largefile
fi
