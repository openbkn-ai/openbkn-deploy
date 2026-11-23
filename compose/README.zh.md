# OpenBKN 部署 · docker compose 单机栈

[English](README.md) | 中文

用一套 `docker-compose.yml` 在**单台主机**上起完整的 OpenBKN（网关 + 中间件 + 认证 + 全部业务服务），
不需要 Kubernetes / Helm。服务名、端口、配置项与 `charts/` 里的 values 一一对齐，
k8s 中写死的服务间地址（如 `bkn-safe:3000`）在 compose 里同名可直接复用。

- 面向**本地 / 单机 / 内网验证**，不是生产部署方案。生产安装用 `deploy.sh`（见[主 README](../README.zh.md)）。
- 与 k8s 部署并存，互不影响：`compose/` 全部是新增文件，不改动 `charts/` 与安装脚本。
- 数据不跨主机：数据卷保存在本机 Docker volume 中，等价于 k8s 里 data-services 的单节点形态。

## 目录

| 路径 | 说明 |
|---|---|
| `docker-compose.yml` | 基础栈：网关、MariaDB、PostgreSQL(hydra)、Redis、OpenSearch、Kafka、MinIO、migrator、hydra/bkn-safe、全部业务服务 |
| `docker-compose.trace.yml` | 可选：BKN Trace（agent-observability + otelcol-contrib） |
| `docker-compose.sandbox.yml` | 可选：沙箱控制面（挂载宿主 `docker.sock` 创建执行器容器） |
| `.env.example` | 环境变量模板（镜像仓库、版本、密码、对外地址） |
| `config/` | 各服务配置文件，由 chart ConfigMap/Secret 逐项翻译而来 |
| `init/` | 一次性初始化脚本（OAuth 客户端注册、默认对象存储登记、凭据渲染） |
| `gateway/nginx.conf` | 反向代理，等价 k8s Ingress，前缀与 chart `ingress.path` 一致 |

## 前置条件

- Docker Engine 24+，Compose v2（`docker compose version`）。想用 `--wait` 需 2.20+。
- 至少 8 GiB 可用内存（OpenSearch 512 MiB + Kafka + 各服务）。
- 能拉取 `ghcr.io/openbkn-ai/*` 镜像；内网请改 `IMAGE_REGISTRY`。
- `OPENBKN_VERSION` 应与 `release-manifests/<version>/` 中的版本一致。当前默认 `0.2.1`：
  `release-manifests/0.2.2/` 的 chart 已发布，但对应容器镜像尚未全部上架，保持 0.2.1 可确保一次拉起。

## 快速开始

```bash
cd compose
cp .env.example .env
# 至少确认 PUBLIC_BASE_URL / GATEWAY_PORT 与浏览器实际访问地址一致
docker compose up -d
docker compose ps
```

就绪后：

| 入口 | 地址 |
|---|---|
| 网关（唯一对外入口） | `http://localhost:8080` |
| 健康检查 | `http://localhost:8080/healthz` |
| OAuth2 / 登录页 | `http://localhost:8080/oauth2/...`、`http://localhost:8080/login` |
| MinIO 控制台（调试） | `http://localhost:9001` |
| OpenSearch（调试） | `http://localhost:19200` |

业务服务**不映射宿主端口**，全部经网关按前缀转发：
`/api/bkn-backend/`、`/api/bkn-project/`、`/api/ontology-query/`、`/api/vega-backend/`、
`/api/agent-operator-integration/`、`/api/agent-retrieval/`、`/api/mf-model-api/`、
`/api/mf-model-manager/`、`/api/oss-gateway-backend/`、`/api/bkn-agent/`、`/api/safe/`。

### 首次登录

种子管理员账号 `admin`，初始密码取 `BKN_SAFE_INITIAL_PASSWORD`（默认 `111111`）。
首次安装后请立即在 UI / 接口中修改。生产部署请勿使用默认值。

## 启动顺序（自动）

`docker compose up -d` 按 `depends_on` 串行推进，无需手工干预：

1. `mariadb` / `postgres` / `redis` / `opensearch` / `kafka` 起来并通过健康检查；
2. `minio` → `minio-init` 建桶（`openbkn`、`sandbox-workspace`）；
3. `data-migrator` 建库（`openbkn`、`safe`、`bkn_trace`）并执行镜像内置的 migrations 快照；
   需要改用本地 migrations（例如调试 bkn-foundry 仓库的 SQL）时，取消 `docker-compose.yml` 中
   `../migrations` 那一行挂载的注释并指向实际目录；
4. `hydra-migrate` → `hydra` → `bkn-safe`（首次启动建 `admin` 用户与角色数据）；
5. `seed-clients` 注册 `openbkn-studio` / `openbkn-cli` / `openbkn-sdk` OAuth 客户端（幂等）；
6. 业务服务：`bkn-backend`、`bkn-project`、`ontology-query`、`vega-backend`、
   `kafka-connect`、`mf-model-api`、`mf-model-manager`、`oss-gateway-backend`、
   `agent-operator-integration`、`agent-retrieval`、`bkn-agent`；
7. `seed-storage` 在 oss-gateway 登记默认 MinIO 存储（幂等）。

查进度：

```bash
docker compose ps
docker compose logs -f data-migrator seed-clients seed-storage
```

## 可选栈

```bash
# BKN Trace（agent-observability + otelcol-contrib）
docker compose -f docker-compose.yml -f docker-compose.trace.yml up -d

# 沙箱控制面（需要宿主 docker.sock；执行器镜像需预先可拉取）
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml --profile sandbox up -d

# 两者同时开
docker compose -f docker-compose.yml -f docker-compose.trace.yml -f docker-compose.sandbox.yml --profile sandbox up -d
```

- trace 栈额外暴露 `18080`（agent-observability API）与 `14317/14318`（OTLP）。
- 沙箱执行器通过 `sandbox_network` 回连网关 `http://gateway:8080`；该网络名硬编码在
  执行器代码中，**不能改名**。
- Linux 上若 `docker.sock` 权限不足，按提示 `group_add` 宿主 docker 组 GID。

## 常用运维

```bash
docker compose down            # 停止并删除容器/网络，保留数据卷
docker compose down -v         # 连数据卷一起删除（彻底重置，含数据库与对象存储）
docker compose logs -f <服务名>
docker compose up -d --force-recreate <服务名>
```

配置文件是 bind mount，改完 `config/` 下文件后重启对应服务即可生效：

```bash
docker compose restart bkn-backend
```

## 改密码 / 换端口

`.env` 里的密码同时被多个组件引用，修改时注意同步：

| 变量 | 影响 |
|---|---|
| `MARIADB_ROOT_PASSWORD` | MariaDB、data-migrator、bkn-safe、各业务服务配置（`config/*/*.yaml` 中的密码是字面量，需要一并改） |
| `KAFKA_SASL_PASSWORD` | Kafka broker、业务服务 env、**`config/kafka-connect/kafka-connect.properties`（字面量）** |
| `OPENSEARCH_PASSWORD` | OpenSearch 客户端（基础栈关闭了安全插件，密码实际不校验；trace 栈会用到） |
| `MINIO_ROOT_USER/PASSWORD` | MinIO、`seed-storage`、sandbox S3 工作区 |
| `PUBLIC_BASE_URL` | hydra issuer、bkn-safe 浏览器地址、OAuth 回调；必须与浏览器地址完全一致 |

端口：`GATEWAY_PORT` 改网关；中间件调试端口见 `.env.example`（默认只绑 `127.0.0.1`）。

## 与 k8s 部署的差异 / 已知限制

- 中间件全部单实例、无高可用；OpenSearch 关闭安全插件（本地免证书）。
- Kafka 单节点 KRaft，同时提供 `PLAINTEXT:9092`（容器内调试）与 `SASL_PLAINTEXT:19092`（业务使用）。
- 业务服务配置为**静态文件**而非 ConfigMap 模板：k8s 里由 `{{ .Values.* }}` 渲染的值已展开成字面量。
- `bkn-project` 的 git 仓库目录用命名卷 `bkn-project-git`（对应 chart 的 `config.gitStore.rootDir=/data/git`）。
- 追踪/日志上报在基础栈中是关闭的（各服务 `otel.trace.enabled=false`），启用 trace 栈后按需打开。
- trace 的 evidence/audit Kafka 消费保持关闭：需要 topic 预建与凭据注入，超出基础栈范围。
- `data-migrator` 使用镜像内置 migrations 快照；k8s 安装脚本会按 chart 版本取同名快照，两者等价。
- 镜像为多架构构建产物；Apple Silicon 上若某镜像缺 arm64 变体，请开 Rosetta 或改用 amd64 机器。
- `docker compose config` 只做静态校验；真实可用性以 `docker compose up -d` + `docker compose ps` 全绿为准。

## 与安装脚本的关系

`deploy.sh` / `preflight.sh` / `onboard.sh` 面向 Kubernetes，行为不变。本目录是并列的
单机路径：同一套业务镜像、同一套服务名与配置项，只是把编排从 Helm + k8s 换成 Compose。
两者不要在同一台主机上争用相同端口（k8s 走 NodePort / Ingress，compose 走 `GATEWAY_PORT`）。
