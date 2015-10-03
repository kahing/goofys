Goofys is a Filey System written in Go

All the backend data is stored on [S3](https://aws.amazon.com/s3/) as
is. It's a Filey System instead of a File System because it makes
minimal effort at being POSIX compliant. Particularly things that are
difficult to support on S3 or would translate into more than one
round-trip would either fail (rename non-empty dir) or faked (no
per-file permission). goofys does not have a on disk data cache, and
consistency model is close-to-open.

List of not yet implemented fuse operations:
  * in terms of syscalls
    * `mkdir`
    * `readlink`
    * `rename`
    * `rmdir`
    * `statfs`
    * `fsync`
    * `chmod`/`utimes`/`ftruncate`
  * in terms of fuse functions
    * `Mkdir`
    * `ReadSymlink`
    * `Rename`
    * `RmDir`
    * `StatFS`
    * `SyncFile`
    * `SetInodeAttributes`

List of non-`POSIX` behaviors:
  * only sequential writes supported
  * does not support appending to a file yet
  * file mode is always `0644` for regular files and `0700` for directories
  * directories link count is always `2`
  * file owner is always the user running goofys
  * `ctime`, `atime` is always the same as `mtime`

Goofys uses the same [fuse binding](https://github.com/jacobsa/fuse)
as [gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse/). Some
skeleton code is copied from `gcsfuse` due to my unfamiliarity with
`Go` and this particular flavor of fuse. Goofys is licensed under the
same license as gcsfuse (Apache 2.0).

References:
  * Data is stored on [Amazon S3](https://aws.amazon.com/s3/)
  * [Amazon SDK for Go](https://github.com/aws/aws-sdk-go)
  * Other related fuse filesystems
    * [s3fs](https://github.com/s3fs-fuse/s3fs-fuse)
    * [gcsfuse](https://github.com/googlecloudplatform/gcsfuse)
  * [Minio Server](https://github.com/minio/minio) is used for `go test`
