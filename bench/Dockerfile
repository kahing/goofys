FROM ubuntu:16.04

WORKDIR /tmp

# ideally I want to clear out all the go deps after but there's no
# way to do that with ADD
ADD . /root/go/src/github.com/kahing/goofys

ENV PATH=$PATH:/usr/local/go/bin:/root/go/bin
RUN /root/go/src/github.com/kahing/goofys/bench/setup_bench.sh

WORKDIR /root/go/src/github.com/kahing/goofys
RUN go get . && go install

ENTRYPOINT ["/root/go/src/github.com/kahing/goofys/bench/run_bench.sh"]
