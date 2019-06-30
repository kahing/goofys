#!/bin/bash

set -o xtrace
set -o errexit

FILE=$1/sparse

dd if=/dev/zero of=$FILE bs=1M count=5
cp --sparse=always $FILE $FILE.2
ls -l $FILE $FILE.2
diff $FILE $FILE.2

dd if=/dev/urandom of=$FILE.3 bs=1 count=1 seek=$((4*1024))
cp --sparse=always $FILE.3 $FILE.4
ls -l $FILE.3 $FILE.4
diff $FILE.3 $FILE.4
