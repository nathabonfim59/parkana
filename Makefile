# Parkana build entry points. Heavy runs (full test suite, race, cgo/DuckDB
# builds) go to Boat VMs via `tm-boat test <worktree> -- make check`.
GO      ?= go
BIN     ?= bin/parkana
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null)
BRANCH   ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
# The C++ runtime (DuckDB) is linked statically so the binary runs on older
# distributions (RHEL 9's libstdc++ is too old), and the pure-Go resolver and
# user lookup keep the cgo resolver out of the binary. DuckDB's
# bindings pass -lstdc++ explicitly, so STATIC_CXX is a directory, searched
# first, that holds only the static libstdc++.a.
CC         ?= cc
BUILD_TAGS ?= netgo,osusergo
STATIC_CXX := $(CURDIR)/.build/static-cxx
LDFLAGS := -X github.com/nathabonfim59/parkana/internal/version.Version=$(VERSION) \
           -X github.com/nathabonfim59/parkana/internal/version.Revision=$(REVISION) \
           -X github.com/nathabonfim59/parkana/internal/version.Branch=$(BRANCH) \
           -extldflags '-L$(STATIC_CXX) -static-libstdc++'

.PHONY: all build test race lint fmt fmt-check tidy-check check run clean packages

all: check build

build:
	@mkdir -p $(STATIC_CXX) && ln -sf "$$($(CC) -print-file-name=libstdc++.a)" $(STATIC_CXX)/libstdc++.a
	$(GO) build -tags '$(BUILD_TAGS)' -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/parkana

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

lint:
	$(GO) vet ./...
	$(GO) tool staticcheck ./...

# Only the repository's own Go files: agent worktrees live under .tty59/.
# find, not git ls-files: Boat snapshots are git archives without .git.
GOFILES = $(shell find . \( -path ./.tty59 -o -path ./.git \) -prune -o -name '*.go' -print)
# The toolchain's own gofmt: an older gofmt earlier in PATH can't parse
# current syntax (generic methods), and its failure must fail the check.
GOFMT ?= $(shell $(GO) env GOROOT)/bin/gofmt

fmt:
	"$(GOFMT)" -w $(GOFILES)

fmt-check:
	@out="$$("$(GOFMT)" -l $(GOFILES))" || exit $$?; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

tidy-check:
	$(GO) mod tidy -diff

# What CI and every milestone review run.
check: fmt-check tidy-check lint race

run: build
	./$(BIN) -data.dir=./data -storage.fs.dir=./data/objects

clean:
	rm -rf bin data

# Linux packages (deb, rpm) from bin/parkana with nfpm, pinned and
# run through `go run` so it is not a module dependency. Build the binary on
# an old-glibc base (the release uses golang:1.27.3-bookworm, glibc 2.36; the binary needs glibc 2.34) so
# the packages run on current LTS distributions.
NFPM     ?= go run github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.48.0
PKG_ARCH ?= amd64
DIST     ?= dist

packages:
	@test -x bin/parkana || { echo "missing bin/parkana: run make build first"; exit 1; }
	mkdir -p $(DIST)
	for p in deb rpm; do \
		PARKANA_VERSION=$(patsubst v%,%,$(VERSION)) PARKANA_ARCH=$(PKG_ARCH) \
			$(NFPM) package --config packaging/nfpm.yaml --packager $$p --target $(DIST)/ || exit 1; \
	done
