-include .env
export

COMPOSE ?= docker compose
PSQL = $(COMPOSE) exec -T timescaledb psql -U ingestor -d telemetry

.PHONY: up down reset logs psql run sim sim-1k sim-5k count rate test bench

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

psql:
	$(COMPOSE) exec timescaledb psql -U ingestor -d telemetry

run:
	go run ./cmd/ingestor

sim:
	go run ./cmd/simulator -devices 100 -hz 1 -duration 60s

sim-1k:
	go run ./cmd/simulator -devices 100 -hz 10 -duration 60s

sim-5k:
	go run ./cmd/simulator -devices 500 -hz 10 -duration 60s

count:
	$(PSQL) -c "SELECT count(*) AS rows FROM telemetry;"

rate:
	$(PSQL) -c "SELECT count(*) AS rows, round((count(*) / GREATEST(extract(epoch FROM (max(time) - min(time))), 1))::numeric, 1) AS rows_per_sec FROM telemetry WHERE time > now() - interval '5 minutes';"

test:
	go test ./... -race

bench:
	go test -bench=. -benchmem ./internal/model