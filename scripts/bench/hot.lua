-- bench/hot.lua — 单一 hot code 反复打，测全命中 cache 极限。
--
-- 用法：HOT_CODE=xF8hWZ wrk -s scripts/bench/hot.lua ...
--
-- wrk 默认不跟随重定向 → 302 状态码就是请求结束，
-- 测的是 slink 服务端的纯处理时间，不会被外网 long_url 拉慢。

local code = os.getenv("HOT_CODE") or error("HOT_CODE env required")

request = function()
  return wrk.format("GET", "/" .. code)
end

done = function(summary, latency, requests)
  io.write(string.format(
    "[hot] reqs=%d duration=%.2fs RPS=%.1f\n",
    summary.requests,
    summary.duration / 1e6,
    summary.requests / (summary.duration / 1e6)))
  io.write(string.format(
    "[hot] latency  P50=%.2fms  P90=%.2fms  P99=%.2fms  max=%.2fms\n",
    latency:percentile(50) / 1000,
    latency:percentile(90) / 1000,
    latency:percentile(99) / 1000,
    latency.max / 1000))
end
