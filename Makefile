# Makefile for DSend
#
# Convenience wrapper for common development, CI, and release tasks:
# build, test, quality checks, and cross-compiled releases.
#
# Run `make` (or `make help`) to list all targets.

export GO111MODULE=on

APP      := dsend
GOOS     := $(shell go env GOOS)
ifeq ($(GOOS),windows)
BINARY   := bin/$(APP).exe
else
BINARY   := bin/$(APP)
endif
PKG_LIST := $(shell go list ./... | grep -v /vendor)

# Release metadata, injected into the binary at link time.
# Defaults come from git; CI overrides them with the actual commit SHA.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD   ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.Version=$(VERSION) \
	-X main.Commit=$(COMMIT) \
	-X main.BuildTime=$(BUILD)

# Terminal colors. Fall back to empty strings when tput is unavailable,
# so output stays clean on CI and bare-bones environments.
GREEN  := $(shell tput -Txterm setaf 2 2>/dev/null || true)
YELLOW := $(shell tput -Txterm setaf 3 2>/dev/null || true)
CYAN   := $(shell tput -Txterm setaf 6 2>/dev/null || true)
RESET  := $(shell tput -Txterm sgr0 2>/dev/null || true)

.DEFAULT_GOAL := help

## @ Development
.PHONY: dev
dev: tidy fmt vet test build ## Run the full local development loop.

.PHONY: all
all: check-quality test build ## Run quality checks, tests, and the build.

## @ Quality
.PHONY: check-quality
check-quality: lint vet fmt ## Run all code quality checks.

.PHONY: lint
lint: ## Run the linter (requires golangci-lint).
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install it with:"; \
		echo "  go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	@golangci-lint run $(PKG_LIST)

.PHONY: vet
vet: ## Run go vet.
	@go vet $(PKG_LIST)

.PHONY: fmt
fmt: ## Fail if any Go source file is not formatted (line-ending agnostic).
	@unformatted=""; \
	for f in $$(find . -name '*.go' -not -path './vendor/*'); do \
		norm=$$(mktemp); \
		gofmt "$$f" | tr -d '\r' > "$$norm"; \
		tr -d '\r' < "$$f" | cmp -s - "$$norm" || unformatted="$$unformatted $$f"; \
		rm -f "$$norm"; \
	done; \
	if [ -n "$$unformatted" ]; then \
		echo "The following files need gofmt:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

.PHONY: fmt-fix
fmt-fix: ## Rewrite Go source files with gofmt (modifies files).
	@gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum.
	@go mod tidy

## @ Test
.PHONY: test
test: ## Run all tests.
	@go test ./...

.PHONY: test-race
test-race: ## Run all tests with the race detector.
	@go test -race ./...

.PHONY: coverage
coverage: ## Run tests with coverage and emit an HTML report.
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report written to coverage.html"

## @ Build
.PHONY: build
build: ## Build the dsend binary into ./bin.
	@mkdir -p bin
	@go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/dsend
	@echo "$(GREEN)ok$(RESET) built $(BINARY) (version $(VERSION))"

.PHONY: run
run: build ## Build and run the broker server.
	@$(BINARY) server

.PHONY: release
release: ## Cross-compile release binaries into ./dist.
	@mkdir -p dist
	@set -e; for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do \
		os=$${target%/*}; \
		arch=$${target#*/}; \
		ext=""; \
		if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
		echo "Building $$os/$$arch ..."; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o dist/$(APP)-$$os-$$arch$${ext} ./cmd/dsend; \
	done
	@echo "$(GREEN)ok$(RESET) release binaries written to ./dist"

## @ Maintenance
.PHONY: clean
clean: ## Remove build and test artifacts.
	@go clean
	@rm -rf bin dist coverage.out coverage.html

## @ Help
.PHONY: help
help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  make $(YELLOW)<target>$(RESET)'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) { \
			printf "    $(YELLOW)%-20s$(GREEN)%s$(RESET)\n", $$1, $$2 \
		} else if (/^## @ .*/) { \
			printf "  $(CYAN)%s$(RESET)\n", substr($$1, 5) \
		} \
	}' $(MAKEFILE_LIST)
