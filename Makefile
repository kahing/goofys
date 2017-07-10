run-test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.5.3/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...
