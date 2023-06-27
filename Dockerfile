FROM ubuntu:22.04 as builder
COPY . /build
WORKDIR /build
RUN apt update; apt install -y make git golang-go
RUN make build
FROM ubuntu:22.04
COPY --from=builder /build/goofys /usr/local/bin
COPY run.sh /
RUN chmod +x /run.sh
RUN apt update; apt install -y fuse tini && \
    rm -rf /var/cache/apt/archives /var/lib/apt/lists
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD ["/run.sh"]
