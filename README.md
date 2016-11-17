Goofys is a high-performance, POSIX-ish [Amazon S3](https://aws.amazon.com/s3/) file system written in Go

[![Build Status](https://travis-ci.org/kahing/goofys.svg?branch=master)](https://travis-ci.org/kahing/goofys)
[![Github All Releases](https://img.shields.io/github/downloads/kahing/goofys/total.svg)](https://github.com/kahing/goofys/releases/)
[![Twitter Follow](https://img.shields.io/twitter/follow/s3goofys.svg?style=social&label=Follow)](https://twitter.com/s3goofys)

# Overview

Goofys allows you to mount an S3 bucket as a filey system.

It's a Filey System instead of a File System because goofys strives
for performance first and POSIX second. Particularly things that are
difficult to support on S3 or would translate into more than one
round-trip would either fail (random writes) or faked (no per-file
permission). Goofys does not have a on disk data cache, and
consistency model is close-to-open.

# Usage

Pre-built binaries are available [here](https://github.com/kahing/goofys/releases/). You may also need to install fuse-utils first.

```ShellSession
$ export GOPATH=$HOME/work
$ go get github.com/kahing/goofys
$ go install github.com/kahing/goofys
$ cat > ~/.aws/credentials
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
$ $GOPATH/bin/goofys <bucket> <mountpoint>
$ $GOPATH/bin/goofys <bucket:prefix> <mountpoint> # if you only want to mount objects under a prefix
```

Users can also configure credentials via the
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
or the `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` environment variables.

To mount an S3 bucket on startup, make sure the credential is
configured for `root`, and can add this to `/etc/fstab`:

```
goofys#bucket   /mnt/mountpoint        fuse     _netdev,allow_other,--file-mode=0666    0       0
```

Got more questions? Check out [questions other people asked](https://github.com/kahing/goofys/issues?utf8=%E2%9C%93&q=is%3Aissue%20label%3Aquestion%20)

# Benchmark

Using `--stat-cache-ttl 0 --type-cache-ttl 0` for goofys
`-ostat_cache_expire=1` for s3fs to simulate cold runs. Detail for the
benchmark can be found in
[bench.sh](https://github.com/kahing/goofys/blob/master/bench/bench.sh). [Raw data](https://github.com/kahing/goofys/blob/master/bench/)
is available as well. Test was run on an EC2 hi1.4xlarge in us-west-2a
connecting to a bucket in us-west-2. Units are seconds.

![Benchmark result](/bench/bench.png?raw=true "Benchmark")

(†) riofs does not wait for HTTP response before returning from `release()`, so the create files benchmarks do not measure the right thing for it

# License

Copyright (C) 2015 Ka-Hing Cheung

Licensed under the Apache License, Version 2.0

# Current Status

goofys has only been tested under Linux.

List of non-POSIX behaviors/limitations:
  * only sequential writes supported
  * does not store file mode/owner/group
    * use `--(dir|file)-mode` or `--(uid|gid)` options
  * does not support symlink or hardlink
  * `ctime`, `atime` is always the same as `mtime`
  * cannot rename non-empty directories
  * `unlink` returns success even if file is not present

In addition to the items above, the following supportable but not yet implemented:
  * appending to a file
  * creating files larger than 1TB

# References

  * Data is stored on [Amazon S3](https://aws.amazon.com/s3/)
  * [Amazon SDK for Go](https://github.com/aws/aws-sdk-go)
  * Other related fuse filesystems
    * [s3fs](https://github.com/s3fs-fuse/s3fs-fuse): another popular filesystem for S3
    * [gcsfuse](https://github.com/googlecloudplatform/gcsfuse):
      filesystem for
      [Google Cloud Storage](https://cloud.google.com/storage/). Goofys
      borrowed some skeleton code from this project.
  * [S3Proxy](https://github.com/andrewgaul/s3proxy) is used for `go test`
  * [fuse binding](https://github.com/jacobsa/fuse), also used by `gcsfuse`
