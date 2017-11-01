#!/bin/bash
# first need mount proxys3 to /mnt/s3
TEST_BUCKET_MOUNT_POINT_1=/mnt/s3
TEST_DIR=test111

# set the lsunconsistency dir
#lsunconsistency do open dir,then sleep long
killall /root/go/src/github.com/kahing/goofys/test/rmdirwihtopen/lsunconsistency

function test_rmdir_withfdopen {
    
    mkdir -p  $TEST_BUCKET_MOUNT_POINT_1/$TEST_DIR/test
    mkdir -p  $TEST_BUCKET_MOUNT_POINT_1/$TEST_DIR/test1
    echo "ls /mnt/s3"
    ls -lah $TEST_BUCKET_MOUNT_POINT_1/

    nohup /root/go/src/github.com/kahing/goofys/test/rmdirwihtopen/lsunconsistency $TEST_BUCKET_MOUNT_POINT_1/$TEST_DIR/test a > /dev/null 2>&1 &
    openpid=$!

    rm -rf $TEST_BUCKET_MOUNT_POINT_1/$TEST_DIR/test

    echo "ls /mnt/s3/test111"
    lsout=`ls $TEST_BUCKET_MOUNT_POINT_1/$TEST_DIR`
    echo "$lsout"

    echo "ls -lah /mnt/s3/test111"
    lsout=`ls -lah  $TEST_BUCKET_MOUNT_POINT_1/$TEST_DIR`
    echo "$lsout"

    echo "should only test1 dir"

    echo `kill -9  $openpid`
}
test_rmdir_withfdopen
