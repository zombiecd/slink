.PHONY: help up down logs ps migrate migrate-down psql redis-cli \
        run build test bench cover lint tidy clean fmt vet \
        kafka-cli kafka-topics kafka-bootstrap \
        run-consumer build-consumer \
        spike-up spike-init spike-v2 spike-ch spike-kafka-fixture spike-counts spike-down

# 默认 PG DSN（覆盖：make migrate PG_DSN=...）
PG_DSN ?= postgres://slink:slink@localhost:15432/slink?sslmode=disable

help:
	@echo "slink — common make targets"
	@echo ""
	@echo "  up               启动 docker-compose（PG + Redis + Prom + Grafana + Kafka）"
	@echo "  down             停止 docker-compose"
	@echo "  logs             跟随 docker-compose 日志"
	@echo "  ps               查看依赖容器状态"
	@echo "  migrate          执行 PG migrations"
	@echo "  migrate-down     回滚最新一次 migration"
	@echo "  psql             进入 PG shell"
	@echo "  redis-cli        进入 Redis shell"
	@echo "  kafka-cli        进入 kafka 容器 shell"
	@echo "  kafka-topics     列出所有 topic"
	@echo "  kafka-bootstrap  创建 slink.click_events topic (4 partitions)"
	@echo ""
	@echo "  run            本地启动 slink server"
	@echo "  run-consumer   本地启动 slink consumer (v0.4 Day 15+)"
	@echo "  build          编译 server 到 ./bin/slink"
	@echo "  build-consumer 编译 consumer 到 ./bin/slink-consumer"
	@echo "  test        跑所有单元测试"
	@echo "  bench       跑 benchmark"
	@echo "  cover       生成覆盖率报告"
	@echo "  lint        静态检查"
	@echo "  fmt         go fmt"
	@echo "  vet         go vet"
	@echo "  tidy        go mod tidy"
	@echo "  clean       清理 bin/ 和测试产物"

# ── 基础设施 ────────────────────────────────────────────
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

# ── 数据库 ─────────────────────────────────────────────
migrate:
	@echo "→ Applying migrations to $(PG_DSN)"
	@for f in migrations/*.up.sql; do \
		echo "  applying $$f"; \
		docker compose exec -T postgres psql -U slink -d slink < $$f || exit 1; \
	done
	@echo "✓ migrations applied"

migrate-down:
	@last=$$(ls migrations/*.down.sql | sort | tail -n 1); \
	echo "→ Rolling back $$last"; \
	docker compose exec -T postgres psql -U slink -d slink < $$last
	@echo "✓ rolled back"

psql:
	docker compose exec postgres psql -U slink -d slink

redis-cli:
	docker compose exec redis redis-cli

# ── Kafka ──────────────────────────────────────────────
kafka-cli:
	docker compose exec kafka bash

kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 创建 v0.4 click_events topic（4 partitions，retention 7d，按架构稿 §4）
# 幂等：已存在则跳过（--if-not-exists）
kafka-bootstrap:
	@echo "→ Creating slink.click_events topic (4 partitions, retention 7d)"
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic slink.click_events \
		--partitions 4 \
		--replication-factor 1 \
		--config retention.ms=604800000
	@echo "✓ topic ready"
	@$(MAKE) kafka-topics

# ── Go ─────────────────────────────────────────────────
run:
	@if [ ! -f .env ]; then echo "→ creating .env from .env.example"; cp .env.example .env; fi
	go run ./cmd/server

build:
	@mkdir -p bin
	go build -o bin/slink ./cmd/server
	@echo "✓ built bin/slink"

# ── v0.4 Day 15: Kafka consumer 独立 binary ──────────────
run-consumer:
	@if [ ! -f .env ]; then echo "→ creating .env from .env.example"; cp .env.example .env; fi
	go run ./cmd/consumer

build-consumer:
	@mkdir -p bin
	go build -o bin/slink-consumer ./cmd/consumer
	@echo "✓ built bin/slink-consumer"

test:
	go test -race -count=1 ./...

bench:
	go test -bench=. -benchmem -run=^$$ ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "✓ open coverage.html"

lint:
	@command -v golangci-lint >/dev/null || (echo "Install: brew install golangci-lint"; exit 1)
	golangci-lint run

fmt:
	gofmt -s -w .

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf bin coverage.out coverage.html
	go clean -testcache

# ── v0.5 Day 18: ClickHouse spike（库选型 + 写入模式）──────
# 决策依据：docs/architecture/v0.5-clickhouse.md §4
# 三组 spike 同口径（5M 上限 / 30s 时间窗 / batch=1000 / 同 fixture 池）：
#   1. spike-v2          : clickhouse-go/v2 PrepareBatch（高级）
#   2. spike-ch          : ch-go proto.Col* 列容器复用（low-level Native）
#   3. spike-kafka-engine: CH ENGINE=Kafka + MaterializedView（端到端）
SPIKE_DB ?= slink
SPIKE_PASS ?= slink
SPIKE_DBNAME ?= slink_analytics
SPIKE_CH_HOST ?= localhost
SPIKE_CH_NATIVE_PORT ?= 19000
SPIKE_KAFKA_HOST ?= localhost
SPIKE_KAFKA_PORT ?= 19092

spike-up:
	@echo "→ starting kafka + clickhouse only (not full stack)"
	docker compose up -d kafka clickhouse
	@echo "→ waiting kafka + clickhouse healthy (max 60s)"
	@for i in $$(seq 1 30); do \
		k=$$(docker compose ps --status running -q kafka | wc -l | tr -d ' '); \
		c=$$(docker compose ps --status running -q clickhouse | wc -l | tr -d ' '); \
		if [ "$$k" = "1" ] && [ "$$c" = "1" ]; then \
			ch_ok=$$(docker compose exec -T clickhouse wget -q -O - http://127.0.0.1:8123/ping 2>/dev/null | grep -c Ok); \
			kf_ok=$$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && echo 1 || echo 0); \
			if [ "$$ch_ok" -ge 1 ] && [ "$$kf_ok" = "1" ]; then \
				echo "✓ both healthy in ~$$((i*2))s"; exit 0; \
			fi; \
		fi; \
		sleep 2; \
	done; \
	echo "✗ timeout"; exit 1

spike-init:
	@echo "→ apply migrations/clickhouse/0001 (主表 + skip index)"
	docker compose exec -T clickhouse clickhouse-client --user $(SPIKE_DB) --password $(SPIKE_PASS) -d $(SPIKE_DBNAME) --multiquery < migrations/clickhouse/0001_click_events_ch.up.sql
	@echo "→ apply migrations/clickhouse/0002 (Kafka Engine spike 三表)"
	docker compose exec -T clickhouse clickhouse-client --user $(SPIKE_DB) --password $(SPIKE_PASS) -d $(SPIKE_DBNAME) --multiquery < migrations/clickhouse/0002_kafka_engine_spike.up.sql
	@$(MAKE) kafka-bootstrap
	@echo "✓ spike init done"

spike-v2:
	@echo "→ spike-clickhouse-v2 (clickhouse-go/v2 PrepareBatch)"
	go run ./cmd/spike-clickhouse-v2 -addr $(SPIKE_CH_HOST):$(SPIKE_CH_NATIVE_PORT) -user $(SPIKE_DB) -pass $(SPIKE_PASS) -db $(SPIKE_DBNAME)

spike-ch:
	@echo "→ spike-ch-go (ch-go proto.Col* low-level Native)"
	go run ./cmd/spike-ch-go -addr $(SPIKE_CH_HOST):$(SPIKE_CH_NATIVE_PORT) -user $(SPIKE_DB) -pass $(SPIKE_PASS) -db $(SPIKE_DBNAME)

spike-kafka-fixture:
	@echo "→ spike-kafka-fixture (kgo producer → Kafka topic)"
	go run ./cmd/spike-kafka-fixture -brokers $(SPIKE_KAFKA_HOST):$(SPIKE_KAFKA_PORT) -topic slink.click_events

spike-counts:
	@docker compose exec -T clickhouse clickhouse-client --user $(SPIKE_DB) --password $(SPIKE_PASS) -d $(SPIKE_DBNAME) -q "\
		SELECT 'click_events_ch_spike (v2/ch)' AS t, count() FROM click_events_ch_spike \
		UNION ALL \
		SELECT 'click_events_ch_kafka_target (kafka engine)' AS t, count() FROM click_events_ch_kafka_target"

spike-down:
	@echo "→ stop kafka + clickhouse (volume 保留)"
	docker compose stop kafka clickhouse
