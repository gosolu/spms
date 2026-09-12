GO ?= go
COVER_FILE ?= coverage.out

.PHONY: all check fmt vet lint test race cover bench tidy clean help

all: check

## help: List the available targets
help:
	@echo "Available make commands:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## check: Run every quality gate (fmt, vet, lint, race)
check: fmt vet lint race

## fmt: Format the package with gofmt and gofumpt
fmt:
	$(GO) fmt ./...
	golangci-lint fmt

## vet: Run go vet static analysis
vet:
	$(GO) vet ./...

## lint: Run golangci-lint
lint:
	golangci-lint run --timeout=5m

## test: Run unit tests and examples
test:
	$(GO) test -count=1 ./...

## race: Run unit tests with the race detector
race:
	$(GO) test -race -count=1 -timeout 120s ./...

## cover: Run tests and report the total coverage
cover:
	$(GO) test -coverprofile=$(COVER_FILE) -covermode=atomic ./...
	$(GO) tool cover -func=$(COVER_FILE) | tail -1
	@echo "Run '$(GO) tool cover -html=$(COVER_FILE)' for the full report"

## bench: Run the benchmarks
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./...

## tidy: Prune and verify module dependencies
tidy:
	$(GO) mod tidy
	$(GO) mod verify

## clean: Remove generated artifacts
clean:
	rm -f $(COVER_FILE)
