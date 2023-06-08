FROM ubuntu:22.04 as builder
COPY . /build
WORKDIR /build
RUN apt update; apt install -y make git golang-go
RUN make build
FROM ubuntu:22.04
COPY --from=builder /build/goofys /usr/local/bin
COPY run.sh /
RUN apt update; apt install -y fuse && \
    rm -rf /var/cache/apt/archives /var/lib/apt/lists
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini /run.sh
ENTRYPOINT ["/tini", "-g", "--"]
CMD ["/run.sh"]