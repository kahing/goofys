# Copyright 2018 The Kubernetes Authors.
# Copyright 2022 Google LLC
# Copyright 2023 Preferred Networks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export STAGINGVERSION ?= $(shell git describe --long --tags --match='v*' --dirty 2>/dev/null || git rev-list -n1 HEAD)
export BUILD_DATE ?= $(shell date --iso-8601=minutes)
BINDIR ?= bin
LDFLAGS ?= -s -w -X main.version=${STAGINGVERSION} -X main.builddate=${BUILD_DATE} -extldflags '-static'

DRIVER_BINARY = meta-fuse-csi-plugin
STARTER_BINARY = fuse-starter
FUSERMOUNT3PROXY_BINARY = fusermount3-proxy

REGISTRY ?= ghcr.io/StatCan/goofys/meta-fuse-csi-plugin
DRIVER_IMAGE = ${REGISTRY}/${DRIVER_BINARY}
STARTER_IMAGE = ${REGISTRY}/${STARTER_BINARY}
EXAMPLE_IMAGE = ${REGISTRY}/mfcp-example

DOCKER_BUILD_ARGS ?= --load --build-arg STAGINGVERSION=${STAGINGVERSION}
ifneq ("$(shell docker buildx build --help | grep 'provenance')", "")
DOCKER_BUILD_ARGS += --provenance=false
endif

LOAD_TO_KIND ?= false
PUBLISH_IMAGE ?= false

$(info STAGINGVERSION is ${STAGINGVERSION})
$(info DRIVER_IMAGE is ${DRIVER_IMAGE})
$(info STARTER_IMAGE is ${STARTER_IMAGE})

.PHONY: all build-image-linux-amd64

all: build-driver build-examples

driver:
	mkdir -p ${BINDIR}
	CGO_ENABLED=0 GOOS=linux GOARCH=$(shell dpkg --print-architecture) go build -ldflags "${LDFLAGS}" -o ${BINDIR}/${DRIVER_BINARY} cmd/csi_driver/main.go

fuse-starter:
	mkdir -p ${BINDIR}
	CGO_ENABLED=0 GOOS=linux GOARCH=$(shell dpkg --print-architecture) go build -ldflags "${LDFLAGS}" -o ${BINDIR}/${STARTER_BINARY} cmd/fuse_starter/main.go

fusermount3-proxy:
	mkdir -p ${BINDIR}
	CGO_ENABLED=0 GOOS=linux GOARCH=$(shell dpkg --print-architecture) go build -ldflags "${LDFLAGS}" -o ${BINDIR}/${FUSERMOUNT3PROXY_BINARY} cmd/fusermount3-proxy/main.go

build-driver:
	$(eval IMAGE_NAME := ${DRIVER_IMAGE}:${STAGINGVERSION})
	docker buildx build ${DOCKER_BUILD_ARGS} ${DOCKER_CACHE_ARGS} \
		--file ./cmd/csi_driver/Dockerfile \
		--tag ${IMAGE_NAME} \
		--platform linux/amd64 .
	if [ "${PUBLISH_IMAGE}" = "true" ]; then \
		docker push ${IMAGE_NAME}; \
		docker tag ${IMAGE_NAME} ${DRIVER_IMAGE}:latest; \
		docker push ${DRIVER_IMAGE}:latest; \
	fi
	if [ "${LOAD_TO_KIND}" = "true" ]; then \
		kind load docker-image ${IMAGE_NAME};\
	fi

define build-example-template
ifneq ("$(EXAMPLES)", "")
EXAMPLES += build-example-$(1)-$(2)
else
EXAMPLES := build-example-$(1)-$(2)
endif

.PHONY: build-example-$1-$2
build-example-$(1)-$(2):
	$(eval IMAGE_NAME := ${EXAMPLE_IMAGE}-$1-$2:${STAGINGVERSION})
	docker buildx build ${DOCKER_BUILD_ARGS} ${DOCKER_CACHE_ARGS} \
		--file ./examples/$1/$2/Dockerfile \
		--tag ${IMAGE_NAME} \
		--platform linux/amd64 .
	if [ "${PUBLISH_IMAGE}" = "true" ]; then \
		docker push ${IMAGE_NAME}; \
		docker tag ${IMAGE_NAME} ${EXAMPLE_IMAGE}-$1-$2:latest; \
		docker push ${EXAMPLE_IMAGE}-$1-$2:latest; \
	fi
	if [ "${LOAD_TO_KIND}" = "true" ]; then \
		kind load docker-image ${IMAGE_NAME};\
	fi
endef

$(eval $(call build-example-template,proxy,goofys))

$(info $(EXAMPLES))

.PHONY: build-examples
build-examples: $(EXAMPLES)

define test-example-template
ifneq ("$(EXAMPLES)", "")
EXAMPLE_TESTS += test-example-$(1)-$(2)
else
EXAMPLE_TESTS := test-example-$(1)-$(2)
endif

.PHONY: test-example-$1-$2
test-example-$(1)-$(2):
	./examples/check.sh ./$1/$2 mfcp-example-$1-$2 $3 $4 $5 $6
endef

$(eval $(call test-example-template,proxy,goofys,starter,/test.txt,busybox,/data/test.txt))

.PHONY: test-examples
test-examples: $(EXAMPLE_TESTS)

.PHONY: test-e2e
test-e2e:
	- kind delete cluster
	kind create cluster
	./test_e2e.sh
