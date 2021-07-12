FROM ubuntu:16.04

ENV GOVERSION="1.7.4" \
    GOPATH="/go" \
    GOROOT="/goroot"

RUN apt-get update \
    && apt-get -y install git curl fuse \
    && apt-get install -y ca-certificates \
    && curl https://storage.googleapis.com/golang/go${GOVERSION}.linux-amd64.tar.gz | tar xzf - \
    && mv /go ${GOROOT} \
    && ${GOROOT}/bin/go get github.com/kahing/goofys \
    && apt-get purge -y --auto-remove git curl \
    && rm -rf go${GOVERSION}.linux-amd64.tar.gz ${GOROOT} \
    && apt-get clean

ADD docker-entrypoint.sh /
CMD ["/docker-entrypoint.sh"]
