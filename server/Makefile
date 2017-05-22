.PHONY: rm build dev

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

dev:
	rm -f bin/mid;
	go build -o bin/mid;

run:
	./server

rm:
	rm -f bin/mid-linux-amd64; rm -f ~/linkit/mid-linux-amd64;

build:
	export GOOS=linux; export GOARCH=amd64; \
        go build -ldflags "-s -w" -o bin/mid-linux-amd64; \


cp:
	cp  bin/mid-linux-amd64 ~/linkit; cp dev/mid.yml ~/linkit/

tail:
	tail -10 /var/log/linkit/mid.log


cqrcampaign:
	curl http://localhost:50308/cqr?t=campaign

cqrservice:
	curl http://localhost:50308/cqr?t=service

cqrcontent:
	curl http://localhost:50308/cqr?t=content

cqrservice_content:
	curl http://localhost:50308/cqr?t=service_content

cqrcontent_sent:
	curl http://localhost:50308/cqr?t=content_sent

operators:
	curl http://localhost:50308/cqr?t=operators


tables:
	curl http://localhost:50308/tables

call:
	curl -H 'Content-Type: application/json' -H 'Accept: application/json' --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"SVC.ByHash","params":{ "hash": "f90f2aca5c640289d0a29417bcb63a37"}}' -H 'content-type:text/plain;' http://localhost:50307/
