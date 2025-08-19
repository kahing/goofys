## Benchmark with Azure Blob Storage

The test was run on `Standard DS4 v2` in `West US` connected to a bucket also in `West US`. Units are seconds. For [blobfuse](https://github.com/Azure/azure-storage-fuse/), cache directory is cleared to simulate cold read.

![Azure Benchmark result](/bench/azure/bench.png?raw=true "Azure Benchmark")

To run the benchmark, do:
```ShellSession
$ export AZURE_STORAGE_ACCOUNT=myaccount
$ export AZURE_STORAGE_KEY=STORAGE-ACCESS-KEY
$ sudo docker run -e BUCKET=$TESTBUCKET -e AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY --rm --privileged --net=host -v /mnt/cache:/tmp/cache StatCan/goofys-bench:azure-latest
# result will be written to $TESTBUCKET
```
