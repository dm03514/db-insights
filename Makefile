PKGS = $(shell go list ./... | grep -v /vendor/)

test-unit:
	GO111MODULE=on go test -tags=unit -coverprofile=coverage.out $(PKGS)

build:
	GO111MODULE=on go build -v ./...

build-linux:
	GOOS=linux go build -v -ldflags="-s -w" -o ./build/sinkshim cmd/sink-shim/main.go

build-macos:
	GOOS=darwin go build -v -ldflags="-s -w" -o ./build/sinkshim cmd/sink-shim/main.go

fmt:
	go fmt $(PKGS)
