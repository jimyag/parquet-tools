GIT_TAG := $(shell git describe --tags --always --dirty="-dev")
BUILD_TIME := $(shell date "+%Y-%m-%dT%H:%M:%S%z")
LDFLAGS := -s -w \
	-X github.com/jimmicro/version.GitTag=$(GIT_TAG) \
	-X github.com/jimmicro/version.BuildTime=$(BUILD_TIME)

.PHONY: build
build:
	go build -trimpath -v -ldflags "$(LDFLAGS)" .
