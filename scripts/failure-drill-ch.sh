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

# ── 等 PG/CH 消费追完稳态（lag 归零代理判断） ──────────────────
# Day 24 加：reset_baseline 前先等前一轮 drain 真追完。
#
# 没有用 kafka-consumer-groups.sh --describe 查 lag 的原因：
# - PG consumer 是独立 kgo client，group `slink.click_events.pg_writer`
# - CH Kafka Engine 是 CH 内部消费，group 名由 SETTINGS kafka_group_name 控
# 双源 lag 查询要分别走 kafka 容器 CLI，命令链长且 awk parse 脆弱。
#
# 代理判断：PG consumer.inserted（累计写入）+ CH 主表 count() 在 STABLE_SECS 秒内
# 都不变，视为追完。这种"稳态"信号在 wrk 已停 / drain 期内可靠。
#
# 边界：如果 producer 仍在打消息（理论上 reset_baseline 时不应有），稳态永远不满足，
# 等到 max_wait 超时强制继续 + warn。
wait_for_lag_zero() {
  local max_wait="${1:-90}"      # 默认最多等 90s（drain 60s + 余量）
  local stable_secs="${2:-5}"    # 稳态判断窗口：连续 N 秒不变
  local start=$(date +%s)
  local prev_c_inserted="" prev_ch_rows="" stable_for=0

  if [ "$DRY_RUN" = "1" ]; then
    info "wait_for_lag_zero: dry run skip"
    return 0
  fi

  info "wait_for_lag_zero: 等前一轮 drain 真追完（max ${max_wait}s / stable ${stable_secs}s）"
  while true; do
    local elapsed=$(( $(date +%s) - start ))
    if [ "$elapsed" -ge "$max_wait" ]; then
      warn "wait_for_lag_zero: 超时 ${max_wait}s 强制继续（c_inserted=${prev_c_inserted} ch_rows=${prev_ch_rows} 仍未稳定）"
      return 1
    fi

    local c_inserted ch_rows
    c_inserted=$(curl -sf --max-time 1 --noproxy '*' "${CONSUMER_ADDR}/debug/stats" 2>/dev/null \
      | python3 -c 'import json,sys;print(json.load(sys.stdin).get("inserted",0))' 2>/dev/null || echo 0)
    ch_rows=$(docker exec "${CH_CONTAINER}" clickhouse-client --user "${CH_USER}" --password "${CH_PASSWORD}" -d "${CH_DB}" --query "SELECT count() FROM click_events_ch" 2>/dev/null || echo -1)

    if [ "$c_inserted" = "$prev_c_inserted" ] && [ "$ch_rows" = "$prev_ch_rows" ]; then
      stable_for=$(( stable_for + 1 ))
      if [ "$stable_for" -ge "$stable_secs" ]; then
        info "wait_for_lag_zero: 追完稳态 (c_inserted=${c_inserted} ch_rows=${ch_rows} 稳定 ${stable_secs}s / 耗时 ${elapsed}s)"
        return 0
      fi
    else
      stable_for=0
    fi
    prev_c_inserted="$c_inserted"
    prev_ch_rows="$ch_rows"
    sleep 1
  done
}

# ── baseline reset（每轮独立） ───────────────────────────────────
# Day 22 P3 修复：移除 kafka-topics --delete + --create
#
# 原因：kgo client（cmd/consumer）在 topic delete 后进入永久错误状态，CREATE
# 后未自动重新 join group，PG 落地链路死。CH Kafka Engine 不受影响（自带
# reconnect）。Day 22 P3 第一次跑时撞这个 bug：PG 表 count=0 / CH 5.99M /
# Go consumer.lag_records=5.19M 卡死。详见 docs/bench/day-22-p3-failure-drill-a.md §4.
#
# 修复：只 TRUNCATE PG/CH 主表，不动 topic / 不动 consumer group offset。
# - PG 主表 truncate 后从 0 起，consumer 从 last commit offset 继续消费新事件
# - CH 主表 truncate 后从 0 起，CH Kafka Engine 的 group offset 不受影响
# - broker 上的旧数据仍在 retention 期内，但已被 consumer commit，不会重消费
#
# 副作用：每轮 baseline 不是 broker LEO=0 的"全新"环境。使用 FROM/TO 时间窗口
# 过滤 recon 即可区分前后轮事件（recon-fixture.sh 已支持）。
#
# Day 24 加：TRUNCATE 前先 wait_for_lag_zero。
# 触发场景：上一轮 drain 60s 不够时（CH 大量 lag / consumer 卡顿），不等 lag 0
# 就 truncate，下一轮 baseline 会被前一轮残留消费污染。Day 22 教训：reset_baseline
# 的本意是"恢复干净起点"，不只是清表，还要保证 consumer 真追上了 broker。
reset_baseline() {
  local round="$1"
  bold "═══ Reset baseline for Round ${round}"
  wait_for_lag_zero
  do_run "docker exec ${PG_CONTAINER} psql -U ${PG_USER} -d ${PG_DB} -c 'TRUNCATE click_events;'"
  do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'TRUNCATE TABLE click_events_ch'"
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
    C) info "[t=${INJECT_AT}s] Round C: DETACH kafka 表 + MV (安全模式：先停 offset 推进)"
       # Day 24 修复：Round C 改安全流程
       #
       # 历史背景：Day 23 P2 v3 Round C 单 DETACH MV 跑出 51% 数据丢失（PG 4.13M / CH 2.01M）。
       # 详见 docs/bench/day-23-p2-failure-drill-abc.md + wiki 40-wiki/database/ClickHouse-Kafka-Engine-MV-DETACH-数据丢失.md
       #
       # 根因：Kafka Engine 表 click_events_ch_kafka_main 在 DETACH MV 期间仍持续消费 Kafka topic
       # 并推进 consumer group offset。MV ATTACH 后从 kafka 表 SELECT，但 offset 已过故障窗口，
       # 该段数据永久无法回放。51% ≈ 30s 故障 / 60s 总时长。
       #
       # Round C 的设计本意是验证「分析侧短暂挂掉后能自愈」（与 Round A/B docker stop/pause 同类）。
       # 用单 DETACH MV 模拟实际上是「真破坏性故障」，故障演练赛道选错。
       #
       # 安全模式 = 同时 DETACH kafka 表 + MV，顺序：kafka → MV。
       # - DETACH kafka 表：CH 停止消费 + 不再推 offset commit
       # - DETACH MV：模拟 MV 在线维护
       # - 故障窗口内 broker 上数据照常累积（producer 持续写）
       # - 恢复 ATTACH 顺序反过来：MV → kafka，kafka 表从 last commit offset 续，broker 数据可消费
       do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'DETACH TABLE click_events_ch_kafka_main'"
       do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'DETACH TABLE click_events_ch_main_mv'" ;;
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
    C) info "[t=${RECOVER_AT}s] Round C: ATTACH MV + kafka 表 (反向顺序)"
       # Day 24 修复：恢复顺序与 inject 反过来。
       # - 先 ATTACH MV：让物化视图就位
       # - 后 ATTACH kafka 表：CH 从 last commit offset 续消费，broker 上故障期数据可被 MV 写入主表
       # 顺序反了的话 kafka 表先 ATTACH 会先推一波 offset 到 MV 还没接的"空窗"，造成漏写。
       do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'ATTACH TABLE click_events_ch_main_mv'"
       do_run "docker exec ${CH_CONTAINER} clickhouse-client --user ${CH_USER} --password ${CH_PASSWORD} -d ${CH_DB} --query 'ATTACH TABLE click_events_ch_kafka_main'" ;;
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
