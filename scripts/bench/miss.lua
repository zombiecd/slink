-- bench/miss.lua — 每次请求一个不存在的随机 code，测穿透防护后的稳态。
--
-- 注意：FLUSHDB 后第一波请求是真 miss（DB 回查），
-- 写入空值标记后稳态变成"命中空值标记"路径——也是不打 DB 的快速路径。
--
-- 想测真"全 miss"（每次都打 DB），需要禁用空值缓存或用永远变化的 code 集 +
-- 持续 FLUSHDB。本脚本是"穿透防护工作中的稳态"。
--
-- 用法：wrk -s scripts/bench/miss.lua ...

local chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
local nchars = #chars

math.randomseed(os.time())

local function newCode()
  local s = "Z" -- 加前缀 'Z' 避免和真实 code 撞概率
  for i = 1, 7 do
    local idx = math.random(nchars)
    s = s .. chars:sub(idx, idx)
  end
  return s
end

request = function()
  return wrk.format("GET", "/" .. newCode())
end

done = function(summary, latency, requests)
  io.write(string.format(
    "[miss] reqs=%d duration=%.2fs RPS=%.1f\n",
    summary.requests,
    summary.duration / 1e6,
    summary.requests / (summary.duration / 1e6)))
  io.write(string.format(
    "[miss] latency  P50=%.2fms  P90=%.2fms  P99=%.2fms  max=%.2fms\n",
    latency:percentile(50) / 1000,
    latency:percentile(90) / 1000,
    latency:percentile(99) / 1000,
    latency.max / 1000))
end
