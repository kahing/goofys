FROM ubuntu:16.04

WORKDIR /tmp

# ideally I want to clear out all the go deps after but there's no
# way to do that with ADD

ADD bench/setup_bench.sh /tmp
RUN /tmp/setup_bench.sh

ENV PATH=$PATH:/usr/local/go/bin:/root/go/bin
ADD . /root/go/src/github.com/kahing/goofys
WORKDIR /root/go/src/github.com/kahing/goofys
RUN go get . && go install

ENTRYPOINT ["/root/go/src/github.com/kahing/goofys/bench/run_bench.sh"]
