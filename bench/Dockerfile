FROM ubuntu:16.04
RUN apt-get update && \
    # s3fs dependencies \
    apt-get -y install automake autotools-dev fuse g++ git libcurl4-gnutls-dev libfuse-dev \
            libssl-dev libxml2-dev make pkg-config \
            # riofs dependencies \
            build-essential gcc make automake autoconf libtool pkg-config intltool \
            libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev \
            # for running goofys benchmark \
            python-pip gnuplot imagemagick
# goofys graph generation
RUN pip install uncertainties numpy

WORKDIR /tmp

ENV PATH=$PATH:/usr/local/go/bin
RUN curl -O https://storage.googleapis.com/golang/go1.8.3.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.8.3.linux-amd64.tar.gz

RUN git clone --depth 1 https://github.com/s3fs-fuse/s3fs-fuse.git
RUN cd s3fs-fuse && ./autogen.sh && ./configure && make -j4 && make install

RUN git clone --depth 1 https://github.com/skoobe/riofs
RUN cd riofs && ./autogen.sh && ./configure && make -j4 && make install

ADD . /root/go/src/github.com/kahing/goofys
ENV PATH=$PATH:/root/go/bin
WORKDIR /root/go/src/github.com/kahing/goofys
RUN ls /root/go/src/github.com/kahing/goofys
RUN go get . && go install

ENTRYPOINT /root/go/src/github.com/kahing/goofys/bench/run_bench.sh
