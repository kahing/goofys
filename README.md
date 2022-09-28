# Overview

Goofys allows you to mount an S3 bucket as a filey system.

It's a Filey System instead of a File System because goofys strives
for performance first and POSIX second. Particularly things that are
difficult to support on S3 or would translate into more than one
round-trip would either fail (random writes) or faked (no per-file
permission). Goofys does not have an on disk data cache (checkout
[catfs](https://github.com/dale0525/catfs)), and consistency model is
close-to-open.

# Installation
*Build from source with Go 1.10 or later:

```ShellSession
$ export GOPATH=$HOME/work
$ go get github.com/dale0525/goofys
$ go install github.com/dale0525/goofys
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
