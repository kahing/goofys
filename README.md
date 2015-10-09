Goofys is a Filey-System interface to [S3](https://aws.amazon.com/s3/)

# Overview

Goofys allows you to mount an `S3` bucket as a filey system.

It's a Filey System instead of a File System because it makes minimal
effort at being POSIX compliant. Particularly things that are
difficult to support on `S3` or would translate into more than one
round-trip would either fail (random writes) or faked (no
per-file permission). Goofys does not have a on disk data cache, and
consistency model is close-to-open.

# License

Copyright (C) 2015 Ka-Hing Cheung

Licensed under the Apache License, Version 2.0

Goofys uses the same [fuse binding](https://github.com/jacobsa/fuse)
as [gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse/). Some
skeleton code is copied from `gcsfuse` due to my unfamiliarity with
`Go` and this particular flavor of fuse. `gcsfuse` is also licensed
under Apache License 2.0.

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

# References

  * Data is stored on [Amazon S3](https://aws.amazon.com/s3/)
  * [Amazon SDK for Go](https://github.com/aws/aws-sdk-go)
  * Other related fuse filesystems
    * [s3fs](https://github.com/s3fs-fuse/s3fs-fuse): another popular filesystem for S3
    * [gcsfuse](https://github.com/googlecloudplatform/gcsfuse):
      filesystem for
      [Google Cloud Storage](https://cloud.google.com/storage/)
  * [Minio Server](https://github.com/minio/minio) is used for `go test`
