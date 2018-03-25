run-test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.6.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build -ldflags "-X main.Version=`git rev-parse HEAD`"

install:
	go install -ldflags "-X main.Version=`git rev-parse HEAD`"
