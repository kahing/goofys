#!/bin/bash

mkdir -p ~/.aws
if [[ ! -z "${AWS_ACCESS_KEY_ID}" ]] && [[ ! -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
cat <<EOF > ~/.aws/credentials
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
EOF
fi

trap "umount ${MNT_POINT}" INT TERM

ARGS=""
if [[ ! -z "${UID}" ]]; then
  ARGS="${ARGS} --uid ${UID}"
fi
if [[ ! -z "${GID}" ]]; then
  ARGS="${ARGS} --gid ${GID}"
fi
if [[ ! -z "${OPTION}" ]]; then
  ARGS="${ARGS} -o ${OPTION}"
fi

goofys -f --endpoint $S3_URL $ARGS $S3_BUCKET $MNT_POINT
