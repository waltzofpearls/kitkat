PKG = $$(go list ./... | grep -v -e '/aggregated')

all: build

build:
	protoc --go_out=. aggregated/*.proto
	go build

install:
	go install

test:
	go vet $(PKG)
	go test -race -v -cover -run "$(filter)" $(PKG)

cover:
	@echo "mode: count" > c.out
	@for pkg in $(PKG); do \
		go test -coverprofile c.out.tmp $$pkg; \
		tail -n +2 c.out.tmp >> c.out; \
	done
	go tool cover -html=c.out
