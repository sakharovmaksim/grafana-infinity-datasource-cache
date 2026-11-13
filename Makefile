VERSION  ?= dev
REVISION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
CONFIG   ?= config.example.yml
IMAGE    ?= grafana-infinity-datasource-cache:dev
BIN      := grafana-infinity-datasource-cache
CHART    := charts/grafana-infinity-datasource-cache

.PHONY: run run-redis redis test lint build image chart-lint chart-test

run:            ## in-memory store, no dependencies; sources and warmup from $(CONFIG)
	LOG_LEVEL=debug go run ./cmd/$(BIN) -config $(CONFIG)

run-redis:      ## shared state in local Redis (make redis); second replica: LISTEN_ADDR=:8081 make run-redis
	LOG_LEVEL=debug REDIS_ADDR=localhost:6379 REDIS_DB=1 go run ./cmd/$(BIN) -config $(CONFIG)

redis:
	docker compose up -d redis

test:
	go test -race ./...

lint:
	golangci-lint run ./...

build:
	go build -ldflags "-X main.version=$(VERSION) -X main.revision=$(REVISION)" -o $(BIN) ./cmd/$(BIN)

image:
	docker build --build-arg VERSION=$(VERSION) --build-arg REVISION=$(REVISION) -t $(IMAGE) .

chart-lint:
	helm lint --strict $(CHART) -f $(CHART)/ci/default-values.yaml

chart-test:     ## needs the helm-unittest plugin
	helm unittest $(CHART)
