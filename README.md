Goofys is a Filey-System interface to [S3](https://aws.amazon.com/s3/)

# Overview

Goofys allows you to mount an `S3` bucket as a filey system.

It's a Filey System instead of a File System because it makes minimal
effort at being POSIX compliant. Particularly things that are
difficult to support on `S3` or would translate into more than one
round-trip would either fail (random writes) or faked (no
per-file permission). Goofys does not have a on disk data cache, and
consistency model is close-to-open.

# Usage

```
go install https://github.com/kahing/goofys
aws configure # for credential
$GOPATH/bin/goofys <bucket> <mountpoint>
```

## Credentials

Goofys uses [Amazon SDK for Go](https://github.com/aws/aws-sdk-go) to
communicate with `S3`, and inherits all the credential configurations
from that. Here is a quick reference if you don't want to use the `aws cli`:

```
cat ~/.aws/credentials
[default]
aws_access_key_id = ACCESS_KEY
aws_secret_access_key = SECRET_KEY

export AWS_ACCESS_KEY=<ACCESS_KEY>
export AWS_SECRET_KEY=<SECRET_KEY>
```

See
[Configuring the AWS Command Line Interface](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
for additional details.

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

List of non-`POSIX` behaviors:
  * only sequential writes supported
  * does not support appending to a file yet
  * file mode is always `0644` for regular files and `0700` for directories
  * directories link count is always `2`
  * file owner is always the user running goofys
  * `ctime`, `atime` is always the same as `mtime`
  * cannot rename non-empty directories
  * `unlink` returns success even if file is not present

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
