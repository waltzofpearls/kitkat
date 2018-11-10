all: build

build:
	protoc --go_out=. aggregated/*.proto
	go build

install:
	go install
