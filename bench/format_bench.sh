#!/bin/bash

set -o errexit
#set -o nounset

if [ $# != 2 ]; then
    echo "Usage: $0 data1 data2"
    exit 1
fi

data1=$1
data2=$2

declare -A RESULT
declare -a TESTS

exec 3<$data1
while read name t <&3; do
    if [ "${RESULT[$name]}" == "" ]; then
        RESULT[$name]=""
        TESTS+=($name)
    fi

    prev=${RESULT[$name]}
    RESULT[$name]="$prev $t"
done

declare -A RESULT2

exec 4<$data2
while read name t <&4; do
    if [ "${RESULT2[$name]}" == "" ]; then
        RESULT2[$name]=""
    fi

    prev=${RESULT2[$name]}
    RESULT2[$name]="$prev $t"
done

# output result
for t in ${TESTS[@]}; do
    echo -en "$t\t"
    echo -en ${RESULT[$t]}
    echo -en "\t"
    echo ${RESULT2[$t]}
done > bench.data

./bench_format.py bench.data
