Goofys is a Filey System written in Go

All the backend data is stored on S3 as is. It's a Filey System
instead of a File System because it makes minimal effort at being
POSIX compliant. Particularly things that are difficult to support on
S3 or would translate into more than one round-trip would either fail
(rename non-empty dir) or faked (no per-file permission). goofys does
not have a on disk data cache, and consistency model is close-to-open.

List of implemented fuse operations:
  * GetInodeAttributes
  * LookUpInode
  * ForgetInode
  * OpenDir
  * ReadDir
  * ReleaseDirHandle
  * OpenFile
  * ReadFile
  * ReleaseFileHandle

List of non-POSIX behaviors:
  * directory link count is always 2
  * regular file link count is always 1

Goofys uses the same [fuse binding](https://github.com/jacobsa/fuse)
as [gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse/). Some
skeleton code is copied from gcsfuse due to my unfamiliarity with Go
and this particular flavor of fuse. Goofys is licensed under the same
license as gcsfuse (Apache 2.0).
