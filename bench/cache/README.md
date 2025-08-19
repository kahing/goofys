## Benchmark with caching enabled

The test was run on an EC2 m5.4xlarge in us-west-2a connected to a
bucket in us-west-2. Units are seconds.

Enabling --cache has little impact on write speed (since catfs implements a write-through cache). Read and time to first byte measures cached read. catfs does not cache metadata operations.

![Cached Benchmark result](/bench/cache/bench.png?raw=true "Cached Benchmark")

To run the benchmark, configure EC2's instance role to be able to write to `$TESTBUCKET`, and then do:
```ShellSession
$ export AWS_ACCESS_KEY_ID=AKID1234567890
$ export AWS_SECRET_ACCESS_KEY=MY-SECRET-KEY
$ sudo docker run -e BUCKET=$TESTBUCKET -e CACHE=true --rm --privileged --net=host -v /tmp/cache:/tmp/cache StatCan/goofys-bench
# result will be written to $TESTBUCKET
```
