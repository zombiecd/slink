.PHONY: help up down logs ps migrate migrate-down psql redis-cli \
        run build test bench cover lint tidy clean fmt vet \
        kafka-cli kafka-topics kafka-bootstrap

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
	@echo "  run         本地启动 slink 服务"
	@echo "  build       编译 binary 到 ./bin/slink"
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
