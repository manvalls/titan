# Titan File System

**DISCLAIMER**: Use at your own risk. The authors of this software are not
responsible in any way for your data loss.

## Overview

Titan is a FUSE file system which stores:

- Metadata information, such as modification date, file ownership and
hierarchy in a database such as MySQL
- File contents in an object storage such as Amazon S3 or Wasabi

In addition, it:

- Is distributed
- Keeps a read-ahead, write-around local cache of file contents
- Supports non-sequential reads and writes without any measurable performance
impact
- Doesn't delete file contents right away when removing or modifying files, but
adds them to an internal recycle bin - which can be later cleaned up using the
cli -, allowing you to restore deleted or modified files using only your
database backups
- Supports atomic sequential writes

## Installation

Install FUSE dependencies for your operating system and build from source
using latest Go:

```
$ go install github.com/manvalls/titan/cmd/titan
```

## Basic usage

Export some basic configuration using environment variables:

```
export TITAN_S3_KEY=<your key>
export TITAN_S3_SECRET=<your secret>
export TITAN_S3_REGION=<s3 region>
export TITAN_S3_BUCKET=<s3 bucket>
export TITAN_S3_ENDPOINT=<storage endpoint, e.g s3.wasabisys.com>
export TITAN_DB_URI=<MySQL DSN as seen in go-sql-driver/mysql>
export TITAN_MOUNT_POINT=<file system mount point, e.g /titan>
export TITAN_CACHE_FOLDER=<folder for the local cache, e.g /titan-cache>
```

Setup your file system (first time only):

```
$ titan mkfs
```

And mount it:

```
$ titan mount &
```
