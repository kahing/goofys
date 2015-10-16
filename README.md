Goofys is a Filey-System interface to [S3](https://aws.amazon.com/s3/)

# Overview

Goofys allows you to mount an `S3` bucket as a filey system.

It's a Filey System instead of a File System because goofys strives
for performance first and POSIX second. Particularly things that are
difficult to support on `S3` or would translate into more than one
round-trip would either fail (random writes) or faked (no per-file
permission). Goofys does not have a on disk data cache, and
consistency model is close-to-open.

# Usage

Pre-built binaries are available [here](https://github.com/kahing/goofys/releases/).

```
$ go get github.com/kahing/goofys
$ go install github.com/kahing/goofys
$ cat > ~/.aws/credentials
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
$ $GOPATH/bin/goofys <bucket> <mountpoint>
```

Users can also configure credentials via the
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
or the `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` environment variables.

# Benchmark

Using `--stat-cache-ttl 0 --type-cache-ttl 0` for goofys
`-ostat_cache_expire=1` for s3fs to simulate cold runs. Detail for the
benchmark can be found in
[bench.sh](https://github.com/kahing/goofys/blob/master/bench.sh). [Raw
data](https://github.com/kahing/goofys/blob/master/bench.data) is
available as well.

operation | goofys | s3fs | speedup
---| ------ | ------ | -----
Create 1000 files|49.4 +/- 1.5s|146.0 +/- 15.0s|2.96 +/- 0.32x
Unlink 1000 files|28.1 +/- 0.8s|36.7 +/- 6.2s|1.31 +/- 0.22x
ls with 1000 files|0.21 +/- 0.04s|3.5 +/- 0.6s|16.9 +/- 4.6x
Create 1000 files (parallel)|21.5 +/- 0.4s|134.2 +/- 9.1s|6.2 +/- 0.4x
Unlink 1000 files (parallel)|28.18 +/- 0.35s|38.1 +/- 4.2s|1.35 +/- 0.15x
Write 1GB|51.4 +/- 4.3MB/s|29.7 +/- 2.9MB/s|1.73 +/- 0.22x
Read 1GB|58.9 +/- 4.7MB/s|65.7 +/- 18.9MB/s|0.90 +/- 0.27x
Time to 1st byte|0.0169 +/- 0.0023s|0.98 +/- 0.06s|58.3 +/- 8.7x

# License

Copyright (C) 2015 Ka-Hing Cheung

Licensed under the Apache License, Version 2.0

# Current Status

List of not yet implemented fuse operations:
  * in terms of syscalls
    * `readlink`
    * `chmod`/`utimes`/`ftruncate`
  * in terms of fuse functions
    * `ReadSymlink`
    * `SetInodeAttributes`

List of non-`POSIX` behaviors/limitations:
  * only sequential writes supported
  * does not support appending to a file yet
  * file mode is always `0644` for regular files and `0700` for directories
  * directories link count is always `2`
  * file owner is always the user running goofys
  * `ctime`, `atime` is always the same as `mtime`
  * cannot rename non-empty directories
  * `unlink` returns success even if file is not present
  * can only create files up to 50GB

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
