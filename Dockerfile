# slink server image — v0.6 Day 26 Phase 1
#
# multi-stage：
#   stage 1 (golang:1.24-alpine) 编译 cmd/server，CGO_ENABLED=0 静态链接
#   stage 2 (distroless/static)  最小运行时（~2MB base，无 shell / 无 package mgr）
#
# 构建：docker build -t slink-server:v0.6 .
# 加载到 kind：kind load docker-image slink-server:v0.6 --name slink

# ── Stage 1: builder ──────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /build

# 先 copy go.mod/sum 跑 mod download，让依赖层独立 cache
COPY go.mod go.sum ./
RUN go mod download

# 再 copy 源码（变更频繁的层放后）
COPY cmd/ cmd/
COPY internal/ internal/

# 静态编译（无 CGO，distroless static 兼容）
# -trimpath：去除本机绝对路径，image 可复现
# -ldflags="-s -w"：去 symbol table + DWARF，binary 缩小 ~30%
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/slink-server ./cmd/server

# ── Stage 2: runtime ──────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/slink-server /app/slink-server

# 18080 = SLINK_ADDR 主端口
# 6060  = SLINK_PPROF_ADDR（distroless 内部仍可访问）
EXPOSE 18080 6060

USER nonroot:nonroot
ENTRYPOINT ["/app/slink-server"]
