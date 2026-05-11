#!/usr/bin/env bash
# Day 27 P1 v2: cross-Pod L1 consistency spike with sleep between iters

set -eu

N="${1:-50}"
POD1="http://localhost:18091"
POD2="http://localhost:18092"

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

LATENCIES=$(mktemp)
trap 'rm -f "$LATENCIES"' EXIT

echo "Day 27 P1 cross-Pod L1 spike, N=$N"

FAILED=0
for i in $(seq 1 "$N"); do
  URL="https://example.com/d27/${i}-$(date +%s%N)"
  RESP=$(curl -s --noproxy '*' --max-time 5 -X POST "$POD1/api/links" -H 'Content-Type: application/json' -d "{\"long_url\":\"$URL\"}" 2>/dev/null || true)
  CODE=$(echo "$RESP" | sed -n 's/.*"code":"\([^"]*\)".*/\1/p')
  if [ -z "$CODE" ]; then
    FAILED=$((FAILED+1))
    sleep 0.05
    continue
  fi
  sleep 0.01
  OUT=$(curl -s --noproxy '*' --max-time 5 -o /dev/null -w '%{http_code} %{time_total}' "$POD2/$CODE" 2>/dev/null || echo "0 0")
  HTTP=$(echo "$OUT" | awk '{print $1}')
  TIME=$(echo "$OUT" | awk '{print $2}')
  if [ "$HTTP" != "302" ]; then
    FAILED=$((FAILED+1))
    sleep 0.05
    continue
  fi
  US=$(awk "BEGIN { printf \"%d\", $TIME * 1000000 }")
  echo "$US" >> "$LATENCIES"
done

SUCCESS=$(wc -l < "$LATENCIES" | tr -d ' ')
echo "Total=$N Success=$SUCCESS Failed=$FAILED"

if [ "$SUCCESS" -eq 0 ]; then
  echo "FAIL: 0 samples"
  exit 1
fi

sort -n "$LATENCIES" -o "$LATENCIES"
p50_line=$((SUCCESS * 50 / 100))
p99_line=$((SUCCESS * 99 / 100))
[ "$p50_line" -lt 1 ] && p50_line=1
[ "$p99_line" -lt 1 ] && p99_line=1
P50=$(sed -n "${p50_line}p" "$LATENCIES")
P99=$(sed -n "${p99_line}p" "$LATENCIES")
MAX=$(tail -1 "$LATENCIES")
MIN=$(head -1 "$LATENCIES")

echo "Cross-Pod GET latency: Min=${MIN}us P50=${P50}us P99=${P99}us Max=${MAX}us"

if [ "$P99" -gt 30000 ]; then
  echo "FAIL: P99 ${P99}us > 30000us (sec 8.2 standard)"
  exit 1
fi
echo "PASS: P99 ${P99}us < 30000us (sec 8.2 standard)"
