goofys is a Filey System written in Go. All the backend data is stored
on S3 as is. It's a Filey System instead of a File System because it
makes minimal effort at being POSIX compliant. Particularly things
that are difficult to support on S3 or would translate into more than
one round-trip would either fail (rename non-empty dir) or faked (no
per-file permission). goofys does not have a on disk data cache, and
consistency model is close-to-open.
