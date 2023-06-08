#!/bin/bash -e

mkdir -p /tmp/s3proxy/test
for i in f1 f2 f3 f4; do
    of=/tmp/s3proxy/test/$i
    [ -f $of ] || dd if=/dev/urandom of=$of bs=1M count=1024
done
of=/tmp/s3proxy/test/small
[ -f $of ] || dd if=/dev/urandom of=$of bs=1 count=1234513
./s3proxy.sh --properties $(dirname $0)/test_parallel_read.properties
