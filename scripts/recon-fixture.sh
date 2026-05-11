#!/usr/bin/env bash
# scripts/recon-fixture.sh — v0.5 fixture 端到端对账（PG vs CH）
#
# 用法：
#   ./scripts/recon-fixture.sh                               # 默认窗口 [now-1h, now-5min)
#   FROM='2026-05-09 10:00:00' TO='2026-05-09 11:00:00' ./scripts/recon-fixture.sh
#   STRICT=1 ./scripts/recon-fixture.sh                       # R1 不过即退 1（CI 模式）
#
# 容器前置：
#   - slink-pg up（PG click_events 主表已写）
#   - slink-clickhouse up（CH click_events_ch 主表已写，apply 0001）
#
# 输出：R1 行数对账 + R2 按 code top 100 漂移 + R3 5min 桶分布 + pass/fail
# 退出码：0 通过 / 1 R1 失败 / 2 双侧 lag 警告 / 3 容器/SQL 错误
#
# 关联：docs/bench/day-19-recon-plan.md

set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-slink-pg}"
CH_CONTAINER="${CH_CONTAINER:-slink-clickhouse}"
CH_DB="${CH_DB:-slink_analytics}"
CH_USER="${CH_USER:-slink}"
CH_PASSWORD="${CH_PASSWORD:-slink}"
CH_PORT="${CH_PORT:-18123}"

PG_USER="${PG_USER:-slink}"
PG_DB="${PG_DB:-slink}"

# 默认窗口：最后 1 小时 - 静默 5 分钟，给两侧消费完
DEFAULT_FROM="$(date -u -v-1H -v-5M '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -u -d '1 hour 5 minutes ago' '+%Y-%m-%d %H:%M:%S')"
DEFAULT_TO="$(date -u -v-5M '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -u -d '5 minutes ago' '+%Y-%m-%d %H:%M:%S')"

FROM="${FROM:-$DEFAULT_FROM}"
TO="${TO:-$DEFAULT_TO}"
STRICT="${STRICT:-0}"

# v2 (Day 30 Phase 4.4): AUTO_WINDOW=1 模式
# 当 FROM/TO 都未显式设 + AUTO_WINDOW=1 时，从 PG click_events.max(ts) 反推窗口：
#   FROM = max(ts) - LAST_N_MIN 分钟
#   TO   = max(ts) + 5s（确保 max(ts) 在窗口内）
# 解决 v1 时间窗 bug：v1 默认窗口 [now-1h, now-5min) 在 drill 跑完立即对账时
# 数据时间在 last 5min 区间内 → 窗口算出 0 行 → R1 误报 FAIL（v0.5 retro §3.7 已账）
#
# 用法：AUTO_WINDOW=1 LAST_N_MIN=10 ./scripts/recon-fixture.sh
AUTO_WINDOW="${AUTO_WINDOW:-0}"
LAST_N_MIN="${LAST_N_MIN:-30}"

if [ "$AUTO_WINDOW" = "1" ] && [ -z "${FROM_RAW:-${FROM_OVERRIDE:-}}" ] && [ "$FROM" = "$DEFAULT_FROM" ] && [ "$TO" = "$DEFAULT_TO" ]; then
  # 从 PG 拿灌数据期 max(ts) — 限最近 24h 防全表扫
  AUTO_MAX_TS=$(docker exec "${PG_CONTAINER:-slink-pg}" psql -U "${PG_USER:-slink}" -d "${PG_DB:-slink}" -tAc \
    "SELECT to_char(max(ts) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') FROM click_events WHERE ts >= now() - interval '24 hours';" 2>/dev/null)
  if [ -n "$AUTO_MAX_TS" ] && [ "$AUTO_MAX_TS" != " " ]; then
    # BSD date 参数顺序：date -j [-v ADJUST] -f INPUT_FMT INPUT_VALUE +OUTPUT_FMT
    FROM=$(LC_ALL=C date -u -j "-v-${LAST_N_MIN}M" -f '%Y-%m-%d %H:%M:%S' "$AUTO_MAX_TS" '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || LC_ALL=C date -u -d "$AUTO_MAX_TS - $LAST_N_MIN minutes" '+%Y-%m-%d %H:%M:%S')
    TO=$(LC_ALL=C date -u -j "-v+5S" -f '%Y-%m-%d %H:%M:%S' "$AUTO_MAX_TS" '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || LC_ALL=C date -u -d "$AUTO_MAX_TS + 5 seconds" '+%Y-%m-%d %H:%M:%S')
    printf '\033[36m[recon-v2]\033[0m AUTO_WINDOW=1 推算窗口 [%s, %s) (基于 PG max(ts)=%s, LAST_N_MIN=%s)\n' "$FROM" "$TO" "$AUTO_MAX_TS" "$LAST_N_MIN"
  else
    printf '\033[33m[recon-v2]\033[0m AUTO_WINDOW=1 但 PG click_events 24h 内无数据，沿用默认窗口\n'
  fi
fi

# 漂移阈值
R1_THRESHOLD="0.001"  # 0.1%
R2_THRESHOLD="0.005"  # 0.5%

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '\033[36m[recon]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[recon]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[recon]\033[0m %s\n' "$*"; }
pass() { printf '\033[32m[recon]\033[0m %s\n' "$*"; }

# ── 容器健康检查 ──────────────────────────────────────────────────
check_containers() {
  for c in "$PG_CONTAINER" "$CH_CONTAINER"; do
    if ! docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true; then
      fail "container $c not running"
      exit 3
    fi
  done
}

# ── PG / CH 查询包装 ──────────────────────────────────────────────
# PG: 通过 docker exec 跑 psql -tAc
pg_query() {
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null
}

# CH: 通过容器内 clickhouse-client（避免 host curl 走代理）
ch_query() {
  docker exec "$CH_CONTAINER" clickhouse-client \
    --user "$CH_USER" --password "$CH_PASSWORD" -d "$CH_DB" \
    --query "$1" 2>/dev/null
}

# ── 静默窗口 cutoff 校验 ──────────────────────────────────────────
check_silence_cutoff() {
  info "检查静默窗口 [${FROM}, ${TO})"
  local pg_max_recent ch_max_recent
  pg_max_recent=$(pg_query "SELECT coalesce(extract(epoch FROM (now() - max(ts)))::int, 999999) FROM click_events WHERE ts >= now() - interval '10 minutes';")
  ch_max_recent=$(ch_query "SELECT toUInt64(now() - max(ts)) FROM click_events_ch WHERE ts >= now() - INTERVAL 10 MINUTE")
  ch_max_recent="${ch_max_recent:-999999}"

  info "PG 最新 ts 距 now=${pg_max_recent}s / CH 最新 ts 距 now=${ch_max_recent}s"
  if [ "$pg_max_recent" -lt 30 ] || [ "$ch_max_recent" -lt 30 ]; then
    warn "双侧仍在写入（< 30s 内有数据），等 30s 后重测可能更稳"
    return 2
  fi
  return 0
}

# ── R1 总行数 ─────────────────────────────────────────────────────
run_r1() {
  bold "═══ R1 总行数对账 [${FROM}, ${TO})"
  local pg_count ch_count
  pg_count=$(pg_query "SELECT count(*) FROM click_events WHERE ts >= '${FROM}'::timestamptz AND ts < '${TO}'::timestamptz;")
  ch_count=$(ch_query "SELECT count() FROM click_events_ch WHERE ts >= toDateTime64('${FROM}', 3, 'UTC') AND ts < toDateTime64('${TO}', 3, 'UTC')")

  pg_count="${pg_count:-0}"
  ch_count="${ch_count:-0}"

  printf "  PG click_events:        %'12d\n" "$pg_count"
  printf "  CH click_events_ch:     %'12d\n" "$ch_count"

  if [ "$pg_count" -eq 0 ]; then
    warn "PG 行数 = 0，窗口内无数据，无法对账"
    return 2
  fi

  local diff_abs drift_pct
  diff_abs=$(( ch_count - pg_count ))
  diff_abs="${diff_abs#-}"  # abs
  drift_pct=$(awk -v a="$diff_abs" -v b="$pg_count" 'BEGIN{printf "%.4f", a/b}')

  printf "  漂移                   %s  (阈值 %s)\n" "$drift_pct" "$R1_THRESHOLD"

  if awk -v d="$drift_pct" -v t="$R1_THRESHOLD" 'BEGIN{exit !(d < t)}'; then
    pass "R1 通过：漂移 ${drift_pct} < ${R1_THRESHOLD}"
    return 0
  else
    fail "R1 不过：漂移 ${drift_pct} >= ${R1_THRESHOLD}"
    return 1
  fi
}

# ── R2 按 code 分组 top 100 ───────────────────────────────────────
run_r2() {
  bold "═══ R2 按 code top 100 分组（漂移 > ${R2_THRESHOLD} 列出）"
  local tmp_pg tmp_ch
  tmp_pg=$(mktemp)
  tmp_ch=$(mktemp)
  trap 'rm -f "$tmp_pg" "$tmp_ch"' RETURN

  pg_query "SELECT code, count(*) FROM click_events WHERE ts >= '${FROM}'::timestamptz AND ts < '${TO}'::timestamptz GROUP BY code ORDER BY count(*) DESC LIMIT 100;" \
    | awk -F'|' '{print $1"\t"$2}' | sort > "$tmp_pg"

  ch_query "SELECT code, count() FROM click_events_ch WHERE ts >= toDateTime64('${FROM}', 3, 'UTC') AND ts < toDateTime64('${TO}', 3, 'UTC') GROUP BY code ORDER BY count() DESC LIMIT 100 FORMAT TabSeparated" \
    | sort > "$tmp_ch"

  local total_codes drift_codes
  total_codes=$(wc -l < "$tmp_pg" | tr -d ' ')
  drift_codes=0
  while IFS=$'\t' read -r code pg_c; do
    local ch_c
    ch_c=$(awk -F'\t' -v k="$code" '$1==k{print $2}' "$tmp_ch")
    ch_c="${ch_c:-0}"
    if [ "$pg_c" -eq 0 ]; then continue; fi
    local diff drift
    diff=$(( ch_c - pg_c ))
    diff="${diff#-}"
    drift=$(awk -v a="$diff" -v b="$pg_c" 'BEGIN{printf "%.4f", a/b}')
    if awk -v d="$drift" -v t="$R2_THRESHOLD" 'BEGIN{exit !(d > t)}'; then
      drift_codes=$(( drift_codes + 1 ))
      printf "  %-12s pg=%-10s ch=%-10s drift=%s\n" "$code" "$pg_c" "$ch_c" "$drift"
    fi
  done < "$tmp_pg"

  if [ "$drift_codes" -eq 0 ]; then
    pass "R2 通过：top ${total_codes} code 均在阈值内"
    return 0
  else
    warn "R2 漂移 code 数: ${drift_codes} / ${total_codes}（参考，不阻塞）"
    return 0  # R2/R3 是辅助指标，不阻塞
  fi
}

# ── R3 5min 时间桶 ────────────────────────────────────────────────
run_r3() {
  bold "═══ R3 5min 时间桶分布（参考，列出双侧）"
  local pg_buckets ch_buckets
  pg_buckets=$(pg_query "SELECT date_trunc('hour', ts) + (extract(minute FROM ts)::int / 5) * interval '5 minutes' AS bucket, count(*) FROM click_events WHERE ts >= '${FROM}'::timestamptz AND ts < '${TO}'::timestamptz GROUP BY bucket ORDER BY bucket;")
  ch_buckets=$(ch_query "SELECT toStartOfFiveMinute(ts) AS bucket, count() FROM click_events_ch WHERE ts >= toDateTime64('${FROM}', 3, 'UTC') AND ts < toDateTime64('${TO}', 3, 'UTC') GROUP BY bucket ORDER BY bucket FORMAT TabSeparated")

  printf "  PG 桶:\n%s\n" "$pg_buckets" | sed 's/^/    /'
  printf "  CH 桶:\n%s\n" "$ch_buckets" | sed 's/^/    /'
  pass "R3 已输出供人工核对"
  return 0
}

# ── 失败时 CH 侧诊断 ──────────────────────────────────────────────
diag_ch() {
  bold "═══ CH 诊断（R1 失败时自动跑）"
  info "kafka_consumers lag:"
  ch_query "SELECT topic, partition, num_messages_read FROM system.kafka_consumers WHERE database='${CH_DB}' FORMAT PrettyCompact" || true
  info "system.errors 最近 20 条:"
  ch_query "SELECT name, value, last_error_message FROM system.errors WHERE name LIKE '%Kafka%' OR name LIKE '%MV%' ORDER BY value DESC LIMIT 20 FORMAT PrettyCompact" || true
  info "click_events_ch 表自检:"
  ch_query "SELECT count() AS total_rows, max(ts) AS latest_event FROM click_events_ch FORMAT PrettyCompact" || true
}

# ── 主流程 ────────────────────────────────────────────────────────
main() {
  bold "slink v0.5 fixture 对账 — Day 19 plan"
  echo "FROM=${FROM}"
  echo "TO=${TO}"
  echo "STRICT=${STRICT}"
  echo ""

  check_containers
  check_silence_cutoff || true   # 仅 warn，不退出

  local r1_rc r2_rc r3_rc
  set +e
  run_r1; r1_rc=$?
  echo ""
  run_r2; r2_rc=$?
  echo ""
  run_r3; r3_rc=$?
  set -e

  echo ""
  bold "═══ 汇总"
  printf "  R1 行数:   %s\n" "$( [ "$r1_rc" -eq 0 ] && echo PASS || echo FAIL )"
  printf "  R2 维度:   %s\n" "$( [ "$r2_rc" -eq 0 ] && echo OK   || echo WARN )"
  printf "  R3 时间:   %s\n" "$( [ "$r3_rc" -eq 0 ] && echo OK   || echo WARN )"

  if [ "$r1_rc" -ne 0 ]; then
    echo ""
    diag_ch
    if [ "$STRICT" = "1" ]; then
      exit 1
    fi
  fi

  exit "$r1_rc"
}

main "$@"
