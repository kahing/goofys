#!/bin/bash

: ${TRAVIS:="false"}
: ${FAST:="false"}
: ${test:=""}
: ${CACHE:="false"}

iter=10

if [ "$TRAVIS" != "false" ]; then
    set -o xtrace
    iter=1
fi

if [ "$FAST" != "false" ]; then
    iter=1
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

MOUNTED=0

if [[ "$cmd" == riofs* ]]; then
    RIOFS="true"
else
    RIOFS="false"
fi

$cmd >& mount.log &
PID=$!

function cleanup {
    if [ $MOUNTED == 1 ]; then
        popd >/dev/null
        if [ "$TRAVIS" != "false" ]; then
            rmdir $prefix
        else
            rmdir $prefix >& /dev/null || true # riofs doesn't support rmdir
        fi
    fi

    if [ "$PID" != "" ]; then
        kill $PID >& /dev/null || true
        fusermount -u $mnt >& /dev/null || true
        sleep 1
    fi
}

function cleanup_err {
    err=$?
    cat mount.log
    if [ $MOUNTED == 1 ]; then
        popd >&/dev/null || true
        rmdir $prefix >&/dev/null || true
    fi

    if [ "$PID" != "" ]; then
        kill $PID >& /dev/null || true
        fusermount -u $mnt >& /dev/null || true
    fi

    return $err
}

trap cleanup EXIT
trap cleanup_err ERR

if [ "$TRAVIS" == "false" -a "$cmd" != "cat" ]; then
    for i in $(seq 1 10); do
        if grep -q $mnt /proc/mounts; then
            break
        fi
        sleep 1
    done
    if ! grep -q $mnt /proc/mounts; then
        echo "$mnt not mounted by $cmd"
        cat mount.log
        exit 1
    fi
    MOUNTED=1
else
    # in travis we mount things externally so we know we are mounted
    MOUNTED=1
fi

mkdir -p "$prefix"
pushd "$prefix" >/dev/null

SUDO=
if [ $(id -u) != 0 ]; then
    SUDO=sudo
fi

function drop_cache {
    if [ "$TRAVIS" == "false" ]; then
        (echo 3 | $SUDO tee /proc/sys/vm/drop_caches) > /dev/null
    fi
}

export TIMEFORMAT=%R

function run_test {
    test=$1
    drop_cache
    sleep 2
    if [ "$CACHE" == "false" ]; then
        # make sure riofs cache get cleared
        if [ -d /tmp/cache ]; then
            cache=$(ls -1 /tmp/cache)
            rm -Rf /tmp/cache 2>/dev/null || true
            mkdir -p /tmp/cache/$cache
        fi
    fi
    echo -n "$test "
    if [ $# -gt 1 ]; then
        time $test $2
    else
        time $test
    fi
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
    get_howmany $@
    # people usually use ls in the terminal when color is on
    numfiles=$(ls -1 --color=always | wc -l)
    if [ "$numfiles" != "$howmany" ]; then
        echo "$numfiles != $howmany"
        false
    fi
}

function rm_files {
    get_howmany $@

    for i in $(seq 1 $howmany); do
        rm file$i >&/dev/null || true
    done
}

function find_files {
    numfiles=$(find | wc -l)

    if [ "$numfiles" != 820 ]; then
        echo "$numfiles != 820"
        rm_tree
        exit 1
    fi
}

function create_tree_parallel {
    (for i in $(seq 1 9); do
        mkdir $i
        for j in $(seq 1 9); do
            mkdir $i/$j

            for k in $(seq 1 9); do
                 touch $i/$j/$k & true
             done
         done
    done
    wait)
}

function rm_tree {
    for i in $(seq 1 9); do
        if [ "$TRAVIS" != "false" ]; then
            rm -Rf $i
        else
            rm -Rf $i >& /dev/null || true # riofs doesn't support rmdir
        fi
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
    count=1000
    if [ "$FAST" == "true" ]; then
        count=100
    fi
    dd if=/dev/zero of=largefile bs=1MB count=$count oflag=nocache status=none
}

function read_large_file {
    dd if=largefile of=/dev/null bs=1MB iflag=nocache status=none
}

function read_first_byte {
    dd if=largefile of=/dev/null bs=1 count=1 iflag=nocache status=none
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

function write_md5 {
    seed=$(dd if=/dev/urandom bs=128 count=1 status=none | base64 -w 0)
    random_cmd="openssl enc -aes-256-ctr -pass pass:$seed -nosalt"
    count=1000
    if [ "$FAST" == "true" ]; then
        count=100
    fi
    MD5=$(dd if=/dev/zero bs=1MB count=$count status=none | $random_cmd | \
        tee >(md5sum) >(dd of=largefile bs=1MB oflag=nocache status=none) >/dev/null | cut -f 1 '-d ')
    if [ "$RIOFS" == "true" ]; then
        # riofs doesn't wait for flush, so we need to wait for object to show up
        # XXX kind of broken due to eventual consistency but it's hte best we can do
        while ! aws s3api --endpoint ${ENDPOINT} head-object --bucket ${BUCKET} --key test_dir/largefile >& /dev/null; do sleep 0.1; done
    fi
}

function read_md5 {
    READ_MD5=$(md5sum largefile | cut -f 1 '-d ')
    if [ "$READ_MD5" != "$MD5" ]; then
        echo "$READ_MD5 != $MD5" >&2
        rm largefile
        exit 1
    fi
}

function rm {
    if [ "$RIOFS" == "true" ]; then
        while ! /bin/rm $@; do true; done
    else
        /bin/rm $@
    fi
}

if [ "$t" = "" -o "$t" = "io" ]; then
    for i in $(seq 1 $iter); do
        run_test write_md5
        if [ "$CACHE" != "true" ]; then
            run_test read_md5
            run_test read_first_byte
        fi
        rm largefile
    done
    if [ "$CACHE" = "true" ]; then
        write_md5
        read_md5
        for i in $(seq 1 $iter); do
            run_test read_md5
            run_test read_first_byte
        done
        rm largefile
    fi
fi

if [ "$t" = "" -o "$t" = "ls" ]; then
    create_files_parallel 1000
    for i in $(seq 1 $iter); do
        run_test ls_files 1000
    done
    rm_files 1000
fi

if [ "$t" = "ls_create" ]; then
    create_files_parallel 1000
    test=dummy
    sleep 10
fi

if [ "$t" = "ls_ls" ]; then
    run_test ls_files 1000
fi

if [ "$t" = "ls_rm" ]; then
    rm_files 1000
    test=dummy
fi

if [ "$t" = "" -o "$t" = "find" ]; then
    create_tree_parallel
    for i in $(seq 1 $iter); do
        run_test find_files
    done
    rm_tree
fi

if [ "$t" = "find_create" ]; then
    create_tree_parallel
    test=dummy
    sleep 10
fi

if [ "$t" = "find_find" ]; then
    for i in $(seq 1 $iter); do
        run_test find_files
    done
fi

if [ "$t" = "issue231" ]; then
    run_test write_md5
    (for i in $(seq 1 20); do
         run_test read_md5 & true
    done; wait);
    rm largefile
fi


if [ "$t" = "cleanup" ]; then
    rm -Rf *
    test=dummy
fi

# for https://github.com/kahing/goofys/issues/64
# quote: There are 5 concurrent transfers gong at a time.
# Data file size is often 100-400MB.
# Regarding the number of transfers, I think it's about 200 files.
# We read from the goofys mounted s3 bucket and write to a local spring webapp using curl.
if [ "$t" = "disable" -o "$t" = "issue64" ]; then
    # setup the files
    (for i in $(seq 0 9); do
        dd if=/dev/zero of=file$i bs=1MB count=300 oflag=nocache status=none & true
    done
    wait)
    if [ $? != 0 ]; then
        exit $?
    fi

    # 200 files and 5 concurrent transfer means 40 times, do 50 times for good measure
    (for i in $(seq 0 9); do
        dd if=file$i of=/dev/null bs=1MB iflag=nocache status=none &
    done

    for i in $(seq 10 300); do
        # wait for 1 to finish, then invoke more
        wait -n
        running=$(ps -ef | grep ' dd if=' | grep -v grep | sed 's/.*dd if=file\([0-9]\).*/\1/')
        for i in $(seq 0 9); do
            if echo $running | grep -v -q $i; then
                dd if=file$i of=/dev/null bs=1MB iflag=nocache status=none &
                break
            fi
        done
    done
    wait)
    if [ $? != 0 ]; then
        exit $?
    fi
    
    # cleanup
    (for i in $(seq 0 9); do
        rm -f file$i & true
    done
    wait)
fi

if [ "$test" = "" ]; then
    echo "No test was run: $t"
    exit 1
fi
