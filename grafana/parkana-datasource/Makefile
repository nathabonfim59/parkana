# Backend module targets for the Parkana Grafana data source. The frontend
# scaffold has its own npm targets under src/ and is not handled here.

GO ?= go
GOOS ?= linux
GOARCH ?= amd64
BINARY := gpx_parkana_sql_datasource
DIST := dist

.PHONY: all build backend test vet check clean

all: build

# build produces the Grafana backend binary for the current target platform.
build: backend

backend:
	GO="$(GO)" GOOS="$(GOOS)" GOARCH="$(GOARCH)" ./scripts/build-backend.sh

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

check: vet test

clean:
	rm -rf $(DIST)
