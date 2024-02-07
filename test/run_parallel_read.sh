#!/bin/bash -e

set -u -e

cd "$(dirname "$0")"/..
rm -rf test/output
mkdir test/output
args=''

files="f1 f2 f3 f4"

for i in $files; do
    args="$args mnt/$i test/output/$i"
done

./test/test_parallel_read $args

files="$files small"

cp mnt/small test/output/small

for i in $files; do
    echo "checking $i"
    cmp /tmp/s3proxy/test/$i test/output/$i
done
