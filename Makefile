GO ?= go
BIN := bin

.PHONY: all fmt fmt-check vet lint test race tagtest cover fuzz bench proto build docker up down smoke check verify clean

all: build

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...

test:
	$(GO) test -count=1 -shuffle=on ./...

race:
	$(GO) test -race -count=1 -shuffle=on ./...

tagtest:
	$(GO) test -race -count=1 -tags bufdebug ./internal/...

cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -n 1

fuzz:
	$(GO) test ./internal/encoding -run xxx -fuzz FuzzDecodeEntry -fuzztime 20s

bench:
	$(GO) test -run xxx -bench . -benchmem ./internal/...

proto:
	mkdir -p gen/vegapb
	protoc -I proto --go_out=gen/vegapb --go_opt=paths=source_relative --go-grpc_out=gen/vegapb --go-grpc_opt=paths=source_relative proto/vega.proto

build:
	$(GO) build -o $(BIN)/ ./cmd/...

docker:
	docker compose build

up:
	docker compose up -d --build

down:
	docker compose down

smoke:
	./scripts/docker_smoke.sh

# fast gate: run before EVERY commit
check: fmt-check vet race

# full gate: run at the end (needs Docker and golangci-lint)
verify: fmt-check vet lint race tagtest build cover docker smoke

clean:
	rm -rf $(BIN) coverage.out
