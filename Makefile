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

.PHONY: all build test race lint fmt fmt-check tidy-check check run clean packages packaging-test docs docs-check docs-flags

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
check: fmt-check tidy-check lint packaging-test race

# Rewrite the generated flag tables in docs/configuration.md from the
# FlagSet; TestFlagReference fails make check while they are stale.
docs-flags:
	$(GO) test ./internal/config/ -run TestFlagReference -update

run: build
	./$(BIN) -data.dir=./data -storage.fs.dir=./data/objects

clean:
	rm -rf bin data

# Rewrites the generated metrics table in docs/operations.md from the
# collectors the all-in-one process registers. -count=1: a cached test result
# would skip the rewrite.
.PHONY: docs-metrics
docs-metrics:
	$(GO) test ./cmd/parkana/ -run TestMetricsReference -update -count=1

# Rewrites the generated pattern catalog in docs/pipelines/redaction.md from
# skills/parkana-redaction/patterns.json; TestRedactionCatalogPage fails make
# check while it is stale.
.PHONY: docs-redaction
docs-redaction:
	$(GO) test ./internal/doccheck/ -run TestRedactionCatalogPage -update -count=1

# Linux packages (deb, rpm) from bin/parkana with nfpm, pinned and
# run through `go run` so it is not a module dependency. Build the binary on
# an old-glibc base (the release uses golang:1.27.3-bookworm, glibc 2.36; the binary needs glibc 2.34) so
# the packages run on current LTS distributions.
NFPM     ?= go run github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.48.0
PKG_ARCH ?= amd64
DIST     ?= dist

# The deb and rpm maintainer scripts, run in dpkg's and rpm's order for every
# install, upgrade and remove against a stub systemctl (POSIX sh, no root).
packaging-test:
	sh packaging/test/maintainer-scripts.sh

packages:
	@test -x bin/parkana || { echo "missing bin/parkana: run make build first"; exit 1; }
	mkdir -p $(DIST)
	for p in deb rpm; do \
		PARKANA_VERSION=$(patsubst v%,%,$(VERSION)) PARKANA_ARCH=$(PKG_ARCH) \
			$(NFPM) package --config packaging/nfpm.yaml --packager $$p --target $(DIST)/ || exit 1; \
	done

# The release tarball, dist/parkana-<VERSION>-linux-<PKG_ARCH>.tar.gz: one
# directory of that name holding bin/parkana, README.md, LICENSE,
# CHANGELOG.md and docs/configuration.md. Deterministic (sorted entries,
# mtime = SOURCE_DATE_EPOCH, by default the last commit's time, owner 0:0,
# gzip -n), so two builds of one commit give the same file. Needs GNU tar.
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || date +%s)
TARBALL_NAME := parkana-$(VERSION)-linux-$(PKG_ARCH)

.PHONY: tarball dist-sums
tarball:
	@test -x bin/parkana || { echo "missing bin/parkana: run make build first"; exit 1; }
	mkdir -p $(DIST)
	rm -rf .build/tarball && mkdir -p .build/tarball/$(TARBALL_NAME)
	install -m 0755 bin/parkana .build/tarball/$(TARBALL_NAME)/parkana
	install -m 0644 README.md LICENSE CHANGELOG.md docs/configuration.md .build/tarball/$(TARBALL_NAME)/
	chmod 0755 .build/tarball/$(TARBALL_NAME)
	tar -C .build/tarball --sort=name --mtime=@$(SOURCE_DATE_EPOCH) \
		--owner=0 --group=0 --numeric-owner --format=gnu -cf .build/tarball/out.tar $(TARBALL_NAME)
	gzip -n -9 -c .build/tarball/out.tar > $(DIST)/$(TARBALL_NAME).tar.gz
	rm -rf .build/tarball

# dist/SHA256SUMS rewritten over every release file in dist/.
dist-sums:
	cd $(DIST) && find . -maxdepth 1 -type f ! -name 'SHA256SUMS*' -printf '%f\n' \
		| LC_ALL=C sort | xargs sha256sum > SHA256SUMS.tmp && mv SHA256SUMS.tmp SHA256SUMS

# Documentation site (VitePress: site/, scripts/doc-links.mjs, scripts/docs-gen.mjs). docs installs
# the pinned npm packages and builds the site, generating the HTTP and Go API reference first;
# dead links, dead #anchors and an out-of-date docs/public/llms.txt fail the build. docs-check also
# runs the Go tests that keep the pages in sync with the code (flags, metrics, routes, doc comments).
DOC_TEST_PKGS ?= ./internal/config/ ./internal/doccheck/ ./cmd/parkana/

docs:
	npm ci
	npm run docs:build

docs-check:
	$(GO) test $(DOC_TEST_PKGS)
	npm run docs:build

# Grafana SQL datasource plugin (grafana/parkana-datasource) as a release zip:
# dist/parkana-sql-datasource-<version>.zip, top-level directory = plugin id,
# with its line in dist/SHA256SUMS added or replaced. The frontend builds in
# the plugin's pinned Node container (CONTAINER=podman works too;
# PLUGIN_FRONTEND=host uses npm on the PATH), the backends with CGO_ENABLED=0
# and -trimpath. PLUGIN_FRONTEND_FLAGS=--check also runs typecheck, unit
# tests and lint.
PLUGIN_DIR       := grafana/parkana-datasource
PLUGIN_PLATFORMS ?= linux/amd64 linux/arm64 darwin/arm64 darwin/amd64 windows/amd64

.PHONY: plugin-dist plugin-check
plugin-dist:
	PLUGIN_PLATFORMS='$(PLUGIN_PLATFORMS)' GO='$(GO)' $(PLUGIN_DIR)/scripts/dist.sh '$(VERSION)' '$(DIST)'

# The plugin backend's go vet and go test (its own module).
plugin-check:
	$(MAKE) -C $(PLUGIN_DIR) check GO='$(GO)'
