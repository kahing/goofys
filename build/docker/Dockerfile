FROM golang

RUN apt-get update && \
    # s3fs dependencies \
    apt-get -y install automake autotools-dev fuse g++ git libcurl4-gnutls-dev libfuse-dev \
            libssl-dev libxml2-dev make pkg-config \
            # riofs dependencies \
            build-essential gcc make automake autoconf libtool pkg-config intltool \
            libglib2.0-dev libfuse-dev libxml2-dev libevent-dev libssl-dev

WORKDIR $GOPATH/src/github.com/kahing/goofys

COPY . ../

RUN git clone --depth 1 https://github.com/s3fs-fuse/s3fs-fuse.git
RUN cd s3fs-fuse && ./autogen.sh && ./configure && make -j4 && make install

RUN git clone --depth 1 https://github.com/skoobe/riofs
RUN cd riofs && ./autogen.sh && ./configure && make -j4 && make install

RUN ls $GOPATH/src/github.com/kahing/goofys

RUN git submodule update --init --recursive
RUN go build