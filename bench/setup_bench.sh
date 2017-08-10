#!/bin/bash

set -o verbose
set -o errexit
set -o nounset
set -o pipefail

apt-get update
apt-get -y install --no-install-recommends \
        automake autotools-dev fuse g++ git libcurl4-gnutls-dev libfuse-dev \
        libssl-dev libxml2-dev make pkg-config \
        build-essential gcc make automake autoconf libtool pkg-config intltool \
        libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev \
        curl python-setuptools python-pip gnuplot-nox imagemagick
apt-get clean

pip install uncertainties numpy

curl -O https://storage.googleapis.com/golang/go1.8.3.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.8.3.linux-amd64.tar.gz
rm go1.8.3.linux-amd64.tar.gz

git clone --depth 1 https://github.com/s3fs-fuse/s3fs-fuse.git
pushd s3fs-fuse
./autogen.sh
./configure
make -j4 > /dev/null
make install
popd
rm -Rf s3fs-fuse

git clone --depth 1 https://github.com/skoobe/riofs
pushd riofs
./autogen.sh
./configure
make -j4 > /dev/null
make install
popd
rm -Rf riofs

apt-get purge -y automake autotools-dev g++ libcurl4-gnutls-dev libfuse-dev \
        libssl-dev libxml2-dev make pkg-config \
        build-essential gcc make automake autoconf libtool pkg-config intltool \
        libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev
apt-get autoremove -y --purge
