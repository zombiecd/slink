#!/usr/bin/env bash
# bench/run.sh — Day 5 跳转性能探底压测脚本
#
# 三档场景：
#   A: hot   — 单一 hot code（全命中 cache）
#   B: mixed — 100 个 code 池随机（稳态混合命中）
#   C: miss  — 每次随机不存在的 code（穿透防护稳态）
#
# 前置：
#   1. docker compose up（PG + Redis 起着）
#   2. slink server 起着（go run ./cmd/server/，监听 :18080）
#
# 用法：
#   ./scripts/bench/run.sh           # 跑全部
#   ./scripts/bench/run.sh hot       # 只跑 hot
#   ./scripts/bench/run.sh mixed
#   ./scripts/bench/run.sh miss

set -euo pipefail

ADDR="${SLINK_ADDR:-http://localhost:18080}"
DURATION="${BENCH_DURATION:-30s}"
THREADS="${BENCH_THREADS:-4}"
CONNS="${BENCH_CONNS:-256}"
POOL_SIZE="${BENCH_POOL_SIZE:-100}"
CODES_FILE="${CODES_FILE:-/tmp/slink-codes.txt}"
RESULTS_DIR="${RESULTS_DIR:-/tmp/slink-bench}"

mkdir -p "$RESULTS_DIR"

# 屏蔽 macOS 默认代理对 localhost 的拦截
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
NOPROXY=(--noproxy '*')

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '\033[36m[bench]\033[0m %s\n' "$*"; }

check_server() {
  if ! curl -sf "${NOPROXY[@]}" "$ADDR/healthz" > /dev/null; then
    echo "ERROR: $ADDR/healthz 不通。请先启 server: go run ./cmd/server/"
    exit 1
  fi
}

# 创建 N 个测试短链，写入 CODES_FILE
seed_codes() {
  info "Seeding $POOL_SIZE links → $CODES_FILE"
  : > "$CODES_FILE"
  local i=0
  while [ "$i" -lt "$POOL_SIZE" ]; do
    local resp
    resp=$(curl -sf "${NOPROXY[@]}" -X POST -H 'Content-Type: application/json' \
      -d "{\"long_url\":\"https://example.com/bench/$i\"}" \
      "$ADDR/api/links")
    local code
    code=$(echo "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"])')
    echo "$code" >> "$CODES_FILE"
    i=$((i + 1))
  done
  info "Seeded. First 3 codes:"
  head -3 "$CODES_FILE" | sed 's/^/    /'
}

# 预热：把所有 code 都打一次，写进 cache
warmup() {
  info "Warming up cache..."
  while read -r code; do
    curl -s "${NOPROXY[@]}" -o /dev/null "$ADDR/$code"
  done < "$CODES_FILE"
}

# 清空 Redis（仅 miss 场景前用）
flush_redis() {
  info "FLUSHDB on slink-redis"
  docker exec slink-redis redis-cli FLUSHDB > /dev/null
}

run_hot() {
  local code
  code=$(head -1 "$CODES_FILE")
  bold "═══ Scenario A: HOT (single code, all cache hit) — code=$code"
  HOT_CODE="$code" wrk -t"$THREADS" -c"$CONNS" -d"$DURATION" --latency \
    -s scripts/bench/hot.lua "$ADDR" 2>&1 | tee "$RESULTS_DIR/A-hot.txt"
}

run_mixed() {
  bold "═══ Scenario B: MIXED ($POOL_SIZE codes random, ~100% steady-state hit)"
  CODES_FILE="$CODES_FILE" wrk -t"$THREADS" -c"$CONNS" -d"$DURATION" --latency \
    -s scripts/bench/mixed.lua "$ADDR" 2>&1 | tee "$RESULTS_DIR/B-mixed.txt"
}

run_miss() {
  bold "═══ Scenario C: MISS (random non-existent codes, penetration protection on)"
  flush_redis
  wrk -t"$THREADS" -c"$CONNS" -d"$DURATION" --latency \
    -s scripts/bench/miss.lua "$ADDR" 2>&1 | tee "$RESULTS_DIR/C-miss.txt"
}

main() {
  bold "slink Day 5 redirect benchmark"
  echo "addr=$ADDR threads=$THREADS conns=$CONNS duration=$DURATION pool=$POOL_SIZE"
  echo "results → $RESULTS_DIR/"
  echo ""

  check_server

  case "${1:-all}" in
    hot)
      [ ! -s "$CODES_FILE" ] && seed_codes
      warmup
      run_hot
      ;;
    mixed)
      [ ! -s "$CODES_FILE" ] && seed_codes
      warmup
      run_mixed
      ;;
    miss)
      run_miss
      ;;
    all)
      seed_codes
      warmup
      run_hot
      echo ""
      run_mixed
      echo ""
      run_miss
      ;;
    *)
      echo "unknown scenario: $1 (use: hot|mixed|miss|all)"
      exit 1
      ;;
  esac

  bold "Done. Results in $RESULTS_DIR/"
  ls -la "$RESULTS_DIR/"
}

main "$@"
