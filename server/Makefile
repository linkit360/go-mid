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

update_service:
	curl -X POST -H 'Content-Type: application/json' --data-binary '{"type": "service.new", "data": "{\"id\":\"edf52693-97f1-48c2-a59e-eeee4814df02\",\"title\":\"zzzzzzzzz\",\"description\":v"zzzzzzzzzzz\",\"price\":23434,\"contents\":[{\"id\":\"527b8c57-6ee9-4af8-8fa2-180921698765\",\"title\":\"test-content51\",\"name\":\"file\"}],\"sms_on_content\":\"Привет Лена!\"}" }' http://localhost:50319/update
