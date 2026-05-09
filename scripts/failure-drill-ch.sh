#!/usr/bin/env bash
# scripts/failure-drill-ch.sh — v0.5 ClickHouse 故障演练编排（3 轮）
#
# 用法：
#   ./scripts/failure-drill-ch.sh dry        # 仅打印命令序列不执行（设计验证）
#   ./scripts/failure-drill-ch.sh A          # 跑 Round A: stop CH 容器 15s
#   ./scripts/failure-drill-ch.sh B          # 跑 Round B: pause CH 容器 15s
#   ./scripts/failure-drill-ch.sh C          # 跑 Round C: DETACH MV 15s
#   ./scripts/failure-drill-ch.sh all        # 三轮全跑（每轮间 60s 间隔）
#
# 前置：
#   - docker compose up（slink-pg + slink-kafka + slink-clickhouse + ...）
#   - apply migrations: PG 0001-0004 / CH 0001 主表 + 0003 kafka_engine_main（待 Day 20 创建）
#   - server :18080 + consumer :18081 起着
#   - codes 已 seed (/tmp/slink-codes.txt 100 个)
#
# 输出：
#   /tmp/slink-day19/round-{A,B,C}.csv     时序数据 1s 一行
#   /tmp/slink-day19/round-{A,B,C}-wrk.txt wrk 输出
#
# 关联：docs/bench/day-19-failure-drill-plan.md

set -euo pipefail

ADDR="${SLINK_ADDR:-http://localhost:18080}"
SERVER_ADMIN_ADDR="${SERVER_ADMIN_ADDR:-http://127.0.0.1:6060}"  # PProf + /debug/stats + /metrics
CONSUMER_ADDR="${CONSUMER_ADDR:-http://localhost:18081}"
DURATION="${BENCH_DURATION:-60s}"
THREADS="${BENCH_THREADS:-4}"
CONNS="${BENCH_CONNS:-256}"
CODES_FILE="${CODES_FILE:-/tmp/slink-codes.txt}"
RESULTS_DIR="${RESULTS_DIR:-/tmp/slink-day19}"

CH_CONTAINER="${CH_CONTAINER:-slink-clickhouse}"
CH_DB="${CH_DB:-slink_analytics}"
CH_USER="${CH_USER:-slink}"
CH_PASSWORD="${CH_PASSWORD:-slink}"
PG_CONTAINER="${PG_CONTAINER:-slink-pg}"
PG_USER="${PG_USER:-slink}"
PG_DB="${PG_DB:-slink}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-slink-kafka}"

INJECT_AT="${INJECT_AT:-10}"     # t=10s 注入故障
RECOVER_AT="${RECOVER_AT:-25}"   # t=25s 恢复
DRAIN_AFTER_WRK="${DRAIN_AFTER_WRK:-60}"  # wrk 结束后 60s drain

DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$RESULTS_DIR"
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '\033[36m[drill]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[drill]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[drill]\033[0m %s\n' "$*"; }

# ── 命令包装（dry 模式仅打印） ──────────────────────────────────
do_run() {
  if [ "$DRY_RUN" = "1" ]; then
    printf '\033[35m[dry]\033[0m %s\n' "$*"
  else
    eval "$@"
  fi
}

# ── 容器 / endpoint 健康检查 ─────────────────────────────────────
preflight() {
  bold "═══ Preflight"
  if [ "$DRY_RUN" = "1" ]; then
    info "dry run: skip container / endpoint / codes-file checks"
    return 0
  fi
  for c in "$PG_CONTAINER" "$CH_CONTAINER" "$KAFKA_CONTAINER"; do
    if ! docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true; then
      fail "container $c not running"; exit 3
    fi
  done
  curl -sf --noproxy '*' "${ADDR}/healthz" > /dev/null || { fail "server $ADDR not healthy"; exit 3; }
  curl -sf --noproxy '*' "${SERVER_ADMIN_ADDR}/debug/stats" > /dev/null || { fail "server admin $SERVER_ADMIN_ADDR not healthy"; exit 3; }
  curl -sf --noproxy '*' "${CONSUMER_ADDR}/healthz" > /dev/null || { fail "consumer $CONSUMER_ADDR not healthy"; exit 3; }
  [ -s "$CODES_FILE" ] || { fail "codes file $CODES_FILE empty (run scripts/bench/run.sh first to seed)"; exit 3; }
  info "preflight ok"
}

# ── baseline reset（每轮独立） ───────────────────────────────────
reset_baseline() {
  local round="$1"
  bold "═══ Reset baseline for Round ${round}"
  do_run "docker exec ${PG_CONTAINER} psql -U ${PG_USER} -d ${PG_DB} -c 'TRUNCATE click_events;'"
  do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'TRUNCATE TABLE click_events_ch'"
  # kafka topic 重建（清 offset）
  do_run "docker exec ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic slink.click_events --if-exists"
  do_run "docker exec ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic slink.click_events --partitions 4 --replication-factor 1"
  sleep 2
}

# ── 故障注入 / 恢复（按 round） ──────────────────────────────────
inject_fault() {
  local round="$1"
  case "$round" in
    A) info "[t=${INJECT_AT}s] Round A: docker stop ${CH_CONTAINER}"
       do_run "docker stop ${CH_CONTAINER}" ;;
    B) info "[t=${INJECT_AT}s] Round B: docker pause ${CH_CONTAINER}"
       do_run "docker pause ${CH_CONTAINER}" ;;
    C) info "[t=${INJECT_AT}s] Round C: DETACH MV click_events_ch_kafka_mv"
       do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'DETACH TABLE click_events_ch_kafka_mv'" ;;
    *) fail "unknown round $round"; exit 2 ;;
  esac
}
recover_fault() {
  local round="$1"
  case "$round" in
    A) info "[t=${RECOVER_AT}s] Round A: docker start ${CH_CONTAINER}"
       do_run "docker start ${CH_CONTAINER}" ;;
    B) info "[t=${RECOVER_AT}s] Round B: docker unpause ${CH_CONTAINER}"
       do_run "docker unpause ${CH_CONTAINER}" ;;
    C) info "[t=${RECOVER_AT}s] Round C: ATTACH MV click_events_ch_kafka_mv"
       do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'ATTACH TABLE click_events_ch_kafka_mv'" ;;
  esac
}

# ── 1s 一次采集 producer / consumer / CH 写 CSV ──────────────────
sample_loop() {
  local csv="$1"
  local end_ts="$2"
  local phase="$3"
  echo "ts_unix,phase,producer_sent,producer_acked,producer_dropped,producer_errors,producer_healthy,pg_inserted,pg_insert_err,pg_lag,ch_target_rows,ch_lag" > "$csv"
  while [ "$(date +%s)" -lt "$end_ts" ]; do
    local now p_json c_json p_sent p_acked p_dropped p_errors p_healthy c_inserted c_insert_err c_lag ch_rows ch_lag
    now=$(date +%s)
    p_json=$(curl -sf --max-time 1 --noproxy '*' "${SERVER_ADMIN_ADDR}/debug/stats" 2>/dev/null || echo '{}')
    c_json=$(curl -sf --max-time 1 --noproxy '*' "${CONSUMER_ADDR}/debug/stats" 2>/dev/null || echo '{}')
    p_sent=$(echo "$p_json" | python3 -c 'import json,sys;d=json.load(sys.stdin).get("kafka_producer",{});print(d.get("sent",0))' 2>/dev/null || echo 0)
    p_acked=$(echo "$p_json" | python3 -c 'import json,sys;d=json.load(sys.stdin).get("kafka_producer",{});print(d.get("acked",0))' 2>/dev/null || echo 0)
    p_dropped=$(echo "$p_json" | python3 -c 'import json,sys;d=json.load(sys.stdin).get("kafka_producer",{});print(d.get("dropped",0))' 2>/dev/null || echo 0)
    p_errors=$(echo "$p_json" | python3 -c 'import json,sys;d=json.load(sys.stdin).get("kafka_producer",{});print(d.get("errors",0))' 2>/dev/null || echo 0)
    p_healthy=$(echo "$p_json" | python3 -c 'import json,sys;d=json.load(sys.stdin).get("kafka_producer",{});print(1 if d.get("healthy") else 0)' 2>/dev/null || echo 0)
    c_inserted=$(echo "$c_json" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("inserted",0))' 2>/dev/null || echo 0)
    c_insert_err=$(echo "$c_json" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("insert_errors",0))' 2>/dev/null || echo 0)
    c_lag=$(echo "$c_json" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("lag_records",-1))' 2>/dev/null || echo -1)
    ch_rows=$(docker exec "${CH_CONTAINER}" clickhouse-client --user "${CH_USER}" --password "${CH_PASSWORD}" -d "${CH_DB}" --query "SELECT count() FROM click_events_ch" 2>/dev/null || echo -1)
    ch_lag=$(docker exec "${CH_CONTAINER}" clickhouse-client --user "${CH_USER}" --password "${CH_PASSWORD}" -d "${CH_DB}" --query "SELECT sum(num_messages_read) FROM system.kafka_consumers WHERE database='${CH_DB}'" 2>/dev/null || echo -1)
    echo "${now},${phase},${p_sent},${p_acked},${p_dropped},${p_errors},${p_healthy},${c_inserted},${c_insert_err},${c_lag},${ch_rows},${ch_lag}" >> "$csv"
    sleep 1
  done
}

# ── 单轮编排 ────────────────────────────────────────────────────
run_round() {
  local round="$1"
  bold "═══════════ Round ${round} ═══════════"
  reset_baseline "$round"

  local csv="${RESULTS_DIR}/round-${round}.csv"
  local wrk_out="${RESULTS_DIR}/round-${round}-wrk.txt"
  local started_at end_ts

  # warmup 一遍（让 cache 热起来）
  do_run "while read -r code; do curl -s --noproxy '*' -o /dev/null '${ADDR}/'\$code; done < ${CODES_FILE}"

  if [ "$DRY_RUN" = "1" ]; then
    info "[dry] wrk 60s + 故障注入 t=${INJECT_AT}s 恢复 t=${RECOVER_AT}s + drain ${DRAIN_AFTER_WRK}s"
    inject_fault "$round"
    recover_fault "$round"
    return 0
  fi

  started_at=$(date +%s)
  end_ts=$(( started_at + 60 + DRAIN_AFTER_WRK ))

  # 采样后台
  sample_loop "$csv" "$end_ts" "drill" &
  local sampler_pid=$!

  # wrk 后台
  CODES_FILE="$CODES_FILE" wrk -t"$THREADS" -c"$CONNS" -d"$DURATION" --latency \
    -s scripts/bench/mixed.lua "$ADDR" > "$wrk_out" 2>&1 &
  local wrk_pid=$!

  # 故障编排：等到 t=10s 注入，t=25s 恢复
  sleep "$INJECT_AT"
  inject_fault "$round"
  sleep $(( RECOVER_AT - INJECT_AT ))
  recover_fault "$round"

  # 等 wrk 完
  wait "$wrk_pid"
  info "wrk 完成 ($(date +%s) elapsed=$(( $(date +%s) - started_at ))s)"

  # drain
  info "drain ${DRAIN_AFTER_WRK}s..."
  sleep "$DRAIN_AFTER_WRK"

  # 采样停
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true

  bold "Round ${round} done. CSV=${csv}"
  tail -3 "$csv" | sed 's/^/  /'

  # 跑对账
  info "跑 recon-fixture.sh 验证..."
  ./scripts/recon-fixture.sh || warn "recon 不过（Round ${round} 故障期/边缘期可能漂移）"
}

# ── 主流程 ──────────────────────────────────────────────────────
main() {
  local arg="${1:-A}"
  if [ "$arg" = "dry" ]; then
    DRY_RUN=1
    bold "═════ DRY-RUN: 三轮命令序列预览 ═════"
    arg="all"
  fi
  preflight

  case "$arg" in
    A|B|C) run_round "$arg" ;;
    all)
      for r in A B C; do
        run_round "$r"
        if [ "$DRY_RUN" != "1" ] && [ "$r" != "C" ]; then
          info "60s 间隔后开下一轮"
          sleep 60
        fi
      done
      bold "═════ 三轮全部完成 ═════"
      info "数据：$RESULTS_DIR/"
      ls -la "$RESULTS_DIR/"
      ;;
    *) fail "unknown round: $arg (use: dry|A|B|C|all)"; exit 2 ;;
  esac
}

main "$@"
