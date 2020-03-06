<img src="doc/goofys.png" height="32" width="32" align="middle" /> Goofys is a high-performance, POSIX-ish [Amazon S3](https://aws.amazon.com/s3/) file system written in Go

[![Build Status](https://travis-ci.org/kahing/goofys.svg?branch=master)](https://travis-ci.org/kahing/goofys)
[![Github All Releases](https://img.shields.io/github/downloads/kahing/goofys/total.svg)](https://github.com/kahing/goofys/releases/)
[![Twitter Follow](https://img.shields.io/twitter/follow/s3goofys.svg?style=social&label=Follow)](https://twitter.com/s3goofys)

# Overview

Goofys allows you to mount an S3 bucket as a filey system.

It's a Filey System instead of a File System because goofys strives
for performance first and POSIX second. Particularly things that are
difficult to support on S3 or would translate into more than one
round-trip would either fail (random writes) or faked (no per-file
permission). Goofys does not have an on disk data cache (checkout
[catfs](https://github.com/kahing/catfs)), and consistency model is
close-to-open.

# Installation

* On Linux, install via [pre-built binaries](https://github.com/kahing/goofys/releases/latest/download/goofys). You may also need to install fuse-utils first.

* On macOS, install via [Homebrew](https://brew.sh/):

```ShellSession
$ brew cask install osxfuse
$ brew install goofys
```

* Or build from source with Go 1.10 or later:

```ShellSession
$ export GOPATH=$HOME/work
$ go get github.com/kahing/goofys
$ go install github.com/kahing/goofys
```

# Usage

```ShellSession
$ cat ~/.aws/credentials
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
$ $GOPATH/bin/goofys <bucket> <mountpoint>
$ $GOPATH/bin/goofys <bucket:prefix> <mountpoint> # if you only want to mount objects under a prefix
```

Users can also configure credentials via the
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
or the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

To mount an S3 bucket on startup, make sure the credential is
configured for `root`, and can add this to `/etc/fstab`:

```
goofys#bucket   /mnt/mountpoint        fuse     _netdev,allow_other,--file-mode=0666,--dir-mode=0777    0       0
```

See also: [Instruction for Azure Blob Storage, Azure Data Lake Gen1, and Azure Data Lake Gen2](https://github.com/kahing/goofys/blob/master/README-azure.md).

Got more questions? Check out [questions other people asked](https://github.com/kahing/goofys/issues?utf8=%E2%9C%93&q=is%3Aissue%20label%3Aquestion%20)

# Benchmark

Using `--stat-cache-ttl 1s --type-cache-ttl 1s` for goofys
`-ostat_cache_expire=1` for s3fs to simulate cold runs. Detail for the
benchmark can be found in
[bench.sh](https://github.com/kahing/goofys/blob/master/bench/bench.sh). [Raw data](https://github.com/kahing/goofys/blob/master/bench/)
is available as well. The test was run on an EC2 m5.4xlarge in us-west-2a
connected to a bucket in us-west-2. Units are seconds.

![Benchmark result](/bench/bench.png?raw=true "Benchmark")

To run the benchmark, configure EC2's instance role to be able to write to `$TESTBUCKET`, and then do:
```ShellSession
$ sudo docker run -e BUCKET=$TESTBUCKET -e CACHE=false --rm --privileged --net=host -v /tmp/cache:/tmp/cache kahing/goofys-bench
# result will be written to $TESTBUCKET
```

See also: [cached benchmark result](https://github.com/kahing/goofys/blob/master/bench/cache/README.md) and [result on Azure](https://github.com/kahing/goofys/blob/master/bench/azure/README.md).

# License

Copyright (C) 2015 - 2019 Ka-Hing Cheung

Licensed under the Apache License, Version 2.0

# Current Status

goofys has been tested under Linux and macOS.

List of non-POSIX behaviors/limitations:
  * only sequential writes supported
  * does not store file mode/owner/group
    * use `--(dir|file)-mode` or `--(uid|gid)` options
  * does not support symlink or hardlink
  * `ctime`, `atime` is always the same as `mtime`
  * cannot `rename` directories with more than 1000 children
  * `unlink` returns success even if file is not present
  * `fsync` is ignored, files are only flushed on `close`

In addition to the items above, the following are supportable but not yet implemented:
  * creating files larger than 1TB

## Compatibility with non-AWS S3

goofys has been tested with the following non-AWS S3 providers:

* Amplidata / WD ActiveScale
* Ceph (ex: Digital Ocean Spaces, DreamObjects, gridscale)
* EdgeFS
* EMC Atmos
* Google Cloud Storage
* Minio (limited)
* OpenStack Swift
* S3Proxy
* Wasabi
* Scaleway

Additionally, goofys also works with the following non-S3 object stores:

* Azure Blob Storage
* Azure Data Lake Gen1
* Azure Data Lake Gen2

# References

  * Data is stored on [Amazon S3](https://aws.amazon.com/s3/)
  * [Amazon SDK for Go](https://github.com/aws/aws-sdk-go)
  * Other related fuse filesystems
    * [catfs](https://github.com/kahing/catfs): caching layer that can be used with goofys
    * [s3fs](https://github.com/s3fs-fuse/s3fs-fuse): another popular filesystem for S3
    * [gcsfuse](https://github.com/googlecloudplatform/gcsfuse):
      filesystem for
      [Google Cloud Storage](https://cloud.google.com/storage/). Goofys
      borrowed some skeleton code from this project.
  * [S3Proxy](https://github.com/andrewgaul/s3proxy) is used for `go test`
  * [fuse binding](https://github.com/jacobsa/fuse), also used by `gcsfuse`
