#!/bin/bash

set -o errexit
set -o nounset

if [ $# != 12]; then
    echo "Usage: $0 <mount cmd> <dir>"
    exit 1
fi

cmd=$1
prefix=$2/test_dir

$cmd >& /dev/null &
PID=$!

function cleanup {
    rmdir $prefix

    if [ "$PID" != "" ]; then
        kill $PID
    fi
}

trap cleanup EXIT

mkdir "$prefix"
cd "$prefix"

function drop_cache {
    (echo 3 | sudo tee /proc/sys/vm/drop_caches) > /dev/null
}

TIME="time -f %e"
export TIMEFORMAT=%R

function run_test {
    t=$1
    drop_cache &
    sleep 1
    wait
    echo -n "$t "
    time $t &>/dev/null
}

function create_files {
    for i in $(seq 1 1000); do
        echo $i > file$i
    done
}

function ls_files {
    ls -1 | wc -l
}

function rm_files {
    for i in $(seq 1 1000); do
        rm file$i
    done
}

function create_files_parallel {
    for i in $(seq 1 1000); do
        echo $i > file$i & true
    done
    wait
}

function rm_files_parallel {
    for i in $(seq 1 1000); do
        rm file$i & true
    done
    wait
}

function write_large_file {
    dd if=/dev/zero of=largefile bs=1MB count=1000
}

function read_large_file {
    dd if=largefile of=/dev/null bs=1MB count=1000
}

function read_first_byte {
    dd if=largefile of=/dev/null bs=1 count=1
}

create_files
for i in $(seq 1 10); do
    run_test ls_files
done

for i in $(seq 1 3); do
    run_test rm_files
    run_test create_files
done
rm_files

for i in $(seq 1 3); do
    run_test create_files_parallel
    run_test rm_files_parallel
done

for i in $(seq 1 10); do
    run_test write_large_file
    run_test read_large_file
    run_test read_first_byte
    rm largefile
done
