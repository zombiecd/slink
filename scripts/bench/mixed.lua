-- bench/mixed.lua — 从 codes.txt 随机抽 code，测稳态混合命中率。
--
-- 预热后 100 个 code 全部入 cache，运行期 ~100% 命中（接近 hot 但分布更真实）。
-- 第一次扫描时会有 100 次 miss，之后稳态命中。
--
-- 用法：CODES_FILE=/tmp/slink-codes.txt wrk -s scripts/bench/mixed.lua ...

local file = os.getenv("CODES_FILE") or "/tmp/slink-codes.txt"
local codes = {}
local f = io.open(file, "r")
if not f then error("cannot open " .. file) end
for line in f:lines() do
  if #line > 0 then table.insert(codes, line) end
end
f:close()
if #codes == 0 then error("no codes loaded from " .. file) end

local n = #codes
math.randomseed(os.time())

request = function()
  local c = codes[math.random(n)]
  return wrk.format("GET", "/" .. c)
end

done = function(summary, latency, requests)
  io.write(string.format(
    "[mixed] reqs=%d duration=%.2fs RPS=%.1f code_pool=%d\n",
    summary.requests,
    summary.duration / 1e6,
    summary.requests / (summary.duration / 1e6),
    n))
  io.write(string.format(
    "[mixed] latency  P50=%.2fms  P90=%.2fms  P99=%.2fms  max=%.2fms\n",
    latency:percentile(50) / 1000,
    latency:percentile(90) / 1000,
    latency:percentile(99) / 1000,
    latency.max / 1000))
end
