FROM golang:1.20.7-alpine AS fusermount3-proxy-builder

# Install required build dependencies
RUN apk update && apk upgrade && apk --no-cache add make gcc g++ libc-dev fuse-dev

WORKDIR /meta-fuse-csi-plugin
ADD ./meta-fuse-csi-plugin .
# Build the fusermount3-proxy
RUN make fusermount3-proxy BINDIR=/bin

FROM golang:1.20.7-alpine AS goofys-builder

# Install required build dependencies
RUN apk update && apk upgrade && apk --no-cache add git make gcc g++ libc-dev fuse-dev

WORKDIR /goofys
ADD . .
# Build the goofys app
RUN make build

# 3.20.3 is the latest as of this commit (September 09 2024)
FROM alpine:3.20.3

# Install necessary runtime dependencies
RUN apk update && apk upgrade && apk --no-cache add ca-certificates bash wget

# Download MinIO client (mc)
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/bin/mc && \
    chmod +x /usr/bin/mc && \
    apk del wget && rm -rf /var/cache/apk/*

# Copy the test file
COPY <<EOF /test.txt
This is a test file for minio
EOF

# Copy and configure MinIO
COPY <<EOF /configure_minio.sh
#!/bin/bash
set -eux
/usr/bin/mc alias set k8s-minio-dev http://localhost:9000 minioadmin minioadmin
/usr/bin/mc mb k8s-minio-dev/test-bucket
/usr/bin/mc cp /test.txt k8s-minio-dev/test-bucket
EOF
RUN chmod +x /configure_minio.sh

# Get goofys build from the build stage
COPY --from=goofys-builder /goofys/goofys .

# Get fusermount3-proxy from the build stage and set up symlink
COPY --from=fusermount3-proxy-builder /bin/fusermount3-proxy /usr/bin/fusermount3
RUN ln -sf /usr/bin/fusermount3 /bin/fusermount
