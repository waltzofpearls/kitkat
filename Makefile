all: build

build:
	protoc --go_out=. proto/*.proto
	go build

install:
	go install
