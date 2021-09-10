export CGO_ENABLED=0

run-test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/gaul/s3proxy/releases/download/s3proxy-1.8.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build -ldflags "-X main.Version=`git rev-parse HEAD`"

install:
	go install -ldflags "-X main.Version=`git rev-parse HEAD`"

docker:
	DOCKER_BUILDKIT=1 docker build . -t tnachen/goofys

gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false ./proto/metadata.proto
	protoc --proto_path=proto --python_out=proto ./proto/metadata.proto

