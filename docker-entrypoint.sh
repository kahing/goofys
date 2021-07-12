#!/bin/bash

SHARED_DIR="/var/lib/goofys"

trap "umount ${SHARED_DIR}" SIGKILL SIGTERM SIGHUP SIGINT EXIT

mount --make-shared $SHARED_DIR
OPTS=" -o allow_other -f"

if ! test -z "$GOOFYS_GID"; then
  groupadd -g $GOOFYS_GID goofys
  OPTS="${OPTS} --gid ${GOOFYS_GID}"
fi

if ! test -z "$GOOFYS_UID"; then
  useradd -u $GOOFYS_UID goofys
  OPTS="${OPTS} --uid ${GOOFYS_UID}"
fi

if ! test -z "$GOOFYS_ENDPOINT"; then
  OPTS="${OPTS} --endpoint ${GOOFYS_ENDPOINT}"
fi

$GOPATH/bin/goofys $OPTS $EXTRA_OPTS $BUCKETNAME $SHARED_DIR &

wait
sleep 2
