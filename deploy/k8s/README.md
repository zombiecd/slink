# K8s 部署清单（v0.6 起）

## 文件顺序（前缀数字 = apply 顺序）

| 文件 | 内容 |
|---|---|
| `00-namespace.yaml` | namespace `slink` |
| `10-backend-externalname.yaml` | 4 个 ExternalName Service（pg/redis/kafka/clickhouse → host.docker.internal）|
| `20-deployment.yaml` | slink-server Deployment（Phase 1 replicas=1）|
| `30-service.yaml` | slink-server ClusterIP Service（暴露 18080 / 6060）|

## 一键起栈（Phase 1）

```bash
# 0. 起后端（host docker compose 仍是数据源）
cd ~/x/slink
docker compose up -d
# 等所有 healthy
docker compose ps

# 1. apply migrations（PG + CH）
make migrate-pg  # 或手动 psql
make migrate-ch

# 2. 起 kind 集群（首次）
kind create cluster --name slink

# 3. build image + load to kind
docker build -t slink-server:v0.6 .
kind load docker-image slink-server:v0.6 --name slink

# 4. apply K8s 清单
kubectl apply -f deploy/k8s/

# 5. 等 Pod ready
kubectl -n slink rollout status deployment/slink-server --timeout=60s

# 6. 验证
kubectl -n slink port-forward svc/slink-server 18080:18080 &
curl -sf http://localhost:18080/healthz
curl -X POST -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com"}' \
  http://localhost:18080/api/links
```

## Phase 2-4 待补

- Phase 2：id 号段 → Redis INCRBY；L1 一致性 → 接受短期 miss；优雅停机时序
- Phase 3：replicas=3 + 滚动演练 + 杀 Pod
- Phase 4：OTel collector + NetworkPolicy + Ingress AuthN
