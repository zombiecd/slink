#!/usr/bin/env bash
# Day 27 P1：双 Pod L1 跨 Pod 一致性 spike
#
# 流程：
#   1. POST 到 Pod 1 创建 code → 写 PG + Redis + Pod 1 L1
#   2. 立即 GET 到 Pod 2 → Pod 2 L1 必 miss → 走 Redis 命中（v0.3 LinkCache 设计）→ 回填 Pod 2 L1
#   3. 测 GET 延迟（end-to-end，含 port-forward 网络往返）
#
# 验证 §8.2 spike 兜底：P99 < 30ms
#
# 假设：
#   - kubectl port-forward 已在 18091（Pod 1）/ 18092（Pod 2）后台跑着
#   - docker compose stack healthy（PG + Redis 可达）
#
# 用法：bash scripts/day27-l1-cross-pod.sh [N=100]
set -euo pipefail

N="${1:-100}"
POD1="http://localhost:18091"
POD2="http://localhost:18092"

# 关闭 proxy（避免 502）
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
CURL_OPTS="-s --noproxy *"

echo "═══════════════════════════════════════════════════════════"
echo "Day 27 P1：双 Pod L1 跨 Pod 一致性 spike (N=$N)"
echo "═══════════════════════════════════════════════════════════"

# 预热：确认两 Pod 可达
if ! curl $CURL_OPTS "$POD1/healthz" | grep -q ok; then
  echo "ERROR: Pod 1 ($POD1) not healthy"
  exit 1
fi
if ! curl $CURL_OPTS "$POD2/healthz" | grep -q ok; then
  echo "ERROR: Pod 2 ($POD2) not healthy"
  exit 1
fi
echo "Both pods healthy ✓"

LATENCIES_FILE=$(mktemp)
trap "rm -f $LATENCIES_FILE" EXIT

FAILED=0
for i in $(seq 1 "$N"); do
  # Pod 1 创建 fresh code
  LONG_URL="https://example.com/day27/$i/$(date +%s%N)"
  CREATE_RESP=$(curl $CURL_OPTS -X POST "$POD1/api/links" \
    -H 'Content-Type: application/json' \
    -d "{\"long_url\":\"$LONG_URL\"}" 2>&1)

  CODE=$(echo "$CREATE_RESP" | grep -o '"code":"[^"]*"' | sed 's/"code":"//;s/"//')
  if [ -z "$CODE" ]; then
    echo "[$i] CREATE failed: $CREATE_RESP" >&2
    FAILED=$((FAILED + 1))
    continue
  fi

  # Pod 2 GET（必须 L1 miss + 走 Redis）
  HTTP_CODE_AND_TIME=$(curl $CURL_OPTS -o /dev/null -w "%{http_code} %{time_total}\n" \
    "$POD2/$CODE" 2>&1)
  HTTP_CODE=$(echo "$HTTP_CODE_AND_TIME" | awk '{print $1}')
  TIME_SEC=$(echo "$HTTP_CODE_AND_TIME" | awk '{print $2}')

  if [ "$HTTP_CODE" != "302" ]; then
    echo "[$i] GET expected 302, got $HTTP_CODE for code $CODE" >&2
    FAILED=$((FAILED + 1))
    continue
  fi

  # 转 μs，方便后面排序统计
  TIME_US=$(awk "BEGIN { printf \"%d\", $TIME_SEC * 1000000 }")
  echo "$TIME_US" >> "$LATENCIES_FILE"
done

SUCCESS=$(wc -l < "$LATENCIES_FILE" | tr -d ' ')
echo ""
echo "═══ Results ═══"
echo "Total: $N  Success: $SUCCESS  Failed: $FAILED"

if [ "$SUCCESS" -eq 0 ]; then
  echo "ERROR: 0 successful samples"
  exit 1
fi

sort -n "$LATENCIES_FILE" -o "$LATENCIES_FILE"
P50_LINE=$((SUCCESS * 50 / 100))
P99_LINE=$((SUCCESS * 99 / 100))
[ "$P50_LINE" -lt 1 ] && P50_LINE=1
[ "$P99_LINE" -lt 1 ] && P99_LINE=1

P50=$(sed -n "${P50_LINE}p" "$LATENCIES_FILE")
P99=$(sed -n "${P99_LINE}p" "$LATENCIES_FILE")
MAX=$(tail -1 "$LATENCIES_FILE")
MIN=$(head -1 "$LATENCIES_FILE")

echo "Latency (cross-Pod GET):"
echo "  Min:  ${MIN}μs"
echo "  P50:  ${P50}μs"
echo "  P99:  ${P99}μs"
echo "  Max:  ${MAX}μs"

# §8.2 spike 标准：P99 < 30ms = 30000μs
if [ "$P99" -gt 30000 ]; then
  echo ""
  echo "❌ FAIL: P99 ${P99}μs > 30000μs (§8.2 spike 标准)"
  exit 1
fi

echo ""
echo "✅ PASS: P99 ${P99}μs < 30000μs (§8.2 spike 标准)"
