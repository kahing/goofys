#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage: $0 <dir>"
    exit 1
fi

set -o errexit
set -o nounset

prefix=$1
cd $prefix || exit $?

function drop_cache {
    (echo 3 | sudo tee /proc/sys/vm/drop_caches) > /dev/null
}

function create_files {
    for i in $(seq 1 1000); do
        echo $i > file$i
    done
}

echo "Running create_files"
time create_files

function ls_files {
    ls -1 | wc -l
}

echo "Running ls_files"
drop_cache
sleep 1
time ls_files

drop_cache
sleep 1
time ls_files

drop_cache
sleep 1
time ls_files

function rm_files {
    for i in $(seq 1 1000); do
        rm file$i
    done
}

echo "Running rm_files/create_files"
drop_cache
time rm_files

drop_cache
time create_files
drop_cache
time rm_files

drop_cache
time create_files
drop_cache
time rm_files

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

echo "Running create_files_parallel/rm_files_parallel"

drop_cache
time create_files_parallel
drop_cache
time rm_files_parallel

drop_cache
time create_files_parallel
drop_cache
time rm_files_parallel

drop_cache
time create_files_parallel
drop_cache
time rm_files_parallel

function write_large_file {
    dd if=/dev/zero of=largefile bs=1MB count=1000
}

function read_large_file {
    dd if=largefile of=/dev/null bs=1MB count=1000
}

function read_first_byte {
    dd if=largefile of=/dev/null bs=1 count=1
}

echo "Running write_large_file/read_large_file"
drop_cache
time write_large_file
drop_cache
time read_large_file
drop_cache
time read_first_byte
rm largefile

drop_cache
time write_large_file
drop_cache
time read_large_file
drop_cache
time read_first_byte
rm largefile

drop_cache
time write_large_file
drop_cache
time read_large_file
drop_cache
time read_first_byte
rm largefile
