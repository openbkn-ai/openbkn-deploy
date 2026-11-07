# OpenBKN Deploy · single-host docker compose stack

English | [中文](README.zh.md)

Bring up the complete OpenBKN stack (gateway + middleware + auth + every business service)
on **one host** with a single `docker-compose.yml` — no Kubernetes, no Helm.
Service names, ports and configuration keys mirror the values under `charts/`, so addresses
hardcoded in the k8s charts (e.g. `bkn-safe:3000`) work unchanged here.

- Intended for **local / single-host / on-prem verification**, not production. Use `deploy.sh`
  for production installs (see the [main README](../README.md)).
- Coexists with the k8s flow: everything under `compose/` is new; no chart or installer script is modified.
- Data stays local: state lives in Docker volumes, the single-host equivalent of the k8s data-services layer.

## Layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Base stack: gateway, MariaDB, PostgreSQL (hydra), Redis, OpenSearch, Kafka, MinIO, migrator, hydra/bkn-safe, all business services |
| `docker-compose.trace.yml` | Optional: BKN Trace (agent-observability + otelcol-contrib) |
| `docker-compose.sandbox.yml` | Optional: sandbox control plane (mounts host `docker.sock` to spawn executor containers) |
| `.env.example` | Environment template (registry, version, passwords, public URL) |
| `config/` | Per-service config files, translated item-by-item from chart ConfigMaps/Secrets |
| `init/` | One-shot init jobs (OAuth client registration, default object storage registration, credential rendering) |
| `gateway/nginx.conf` | Reverse proxy equivalent to the k8s Ingress; prefixes match chart `ingress.path` |

## Prerequisites

- Docker Engine 24+, Compose v2 (`docker compose version`); `--wait` needs 2.20+.
- At least 8 GiB free memory (OpenSearch 512 MiB + Kafka + services).
- Ability to pull `ghcr.io/openbkn-ai/*`; set `IMAGE_REGISTRY` for a mirror.
- `OPENBKN_VERSION` should match a version in `release-manifests/<version>/`. The default is
  `0.2.1`: the `0.2.2` charts are published but their container images are not all available yet,
  so 0.2.1 is the version that comes up end-to-end.

## Quick start

```bash
cd compose
cp .env.example .env
# Make sure PUBLIC_BASE_URL / GATEWAY_PORT match how browsers reach the host
docker compose up -d
docker compose ps
```

Endpoints once healthy:

| Entry | Address |
|---|---|
| Gateway (only public entry) | `http://localhost:8080` |
| Health | `http://localhost:8080/healthz` |
| OAuth2 / login | `http://localhost:8080/oauth2/...`, `http://localhost:8080/login` |
| MinIO console (debug) | `http://localhost:9001` |
| OpenSearch (debug) | `http://localhost:19200` |

Business services expose **no host ports**; the gateway routes by prefix:
`/api/bkn-backend/`, `/api/bkn-project/`, `/api/ontology-query/`, `/api/vega-backend/`,
`/api/agent-operator-integration/`, `/api/agent-retrieval/`, `/api/mf-model-api/`,
`/api/mf-model-manager/`, `/api/oss-gateway-backend/`, `/api/bkn-agent/`, `/api/safe/`.

### First login

Seeded administrator is `admin`; the initial password is `BKN_SAFE_INITIAL_PASSWORD`
(default `111111`). Change it right after install; never keep the default in production.

## Startup order (automatic)

`docker compose up -d` walks `depends_on`; no manual steps:

1. `mariadb` / `postgres` / `redis` / `opensearch` / `kafka` start and pass health checks;
2. `minio` → `minio-init` creates buckets (`openbkn`, `sandbox-workspace`);
3. `data-migrator` creates databases (`openbkn`, `safe`, `bkn_trace`) and runs the migrations
   snapshot baked into the image. To use local migrations instead (e.g. debugging the
   bkn-foundry `migrations/` tree), uncomment the `../migrations` mount in `docker-compose.yml`;
4. `hydra-migrate` → `hydra` → `bkn-safe` (seeds the `admin` user and role data on first start);
5. `seed-clients` registers the `openbkn-studio` / `openbkn-cli` / `openbkn-sdk` OAuth clients (idempotent);
6. business services: `bkn-backend`, `bkn-project`, `ontology-query`, `vega-backend`,
   `kafka-connect`, `mf-model-api`, `mf-model-manager`, `oss-gateway-backend`,
   `agent-operator-integration`, `agent-retrieval`, `bkn-agent`;
7. `seed-storage` registers the default MinIO storage in oss-gateway (idempotent).

Watch progress:

```bash
docker compose ps
docker compose logs -f data-migrator seed-clients seed-storage
```

## Optional stacks

```bash
# BKN Trace (agent-observability + otelcol-contrib)
docker compose -f docker-compose.yml -f docker-compose.trace.yml up -d

# Sandbox control plane (needs host docker.sock; executor images must be pullable)
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml --profile sandbox up -d

# Both
docker compose -f docker-compose.yml -f docker-compose.trace.yml -f docker-compose.sandbox.yml --profile sandbox up -d
```

- The trace stack also exposes `18080` (agent-observability API) and `14317/14318` (OTLP).
- Sandbox executors call back through `sandbox_network` at `http://gateway:8080`; that network
  name is hardcoded in the executor code — **do not rename it**.
- On Linux, if `docker.sock` is not accessible, add the host docker group GID via `group_add`.

## Operations

```bash
docker compose down            # stop/remove containers and networks, keep volumes
docker compose down -v         # also delete volumes (full reset, databases and object storage)
docker compose logs -f <service>
docker compose up -d --force-recreate <service>
```

Config files are bind mounts; after editing anything under `config/`, restart the service:

```bash
docker compose restart bkn-backend
```

## Passwords and ports

Passwords in `.env` are referenced by several components; keep them in sync:

| Variable | Affected |
|---|---|
| `MARIADB_ROOT_PASSWORD` | MariaDB, data-migrator, bkn-safe, every service config (passwords in `config/*/*.yaml` are literals — update them too) |
| `KAFKA_SASL_PASSWORD` | Kafka broker, service env, **`config/kafka-connect/kafka-connect.properties` (literal)** |
| `OPENSEARCH_PASSWORD` | OpenSearch clients (security plugin is off in the base stack, so it is not enforced; the trace stack uses it) |
| `MINIO_ROOT_USER/PASSWORD` | MinIO, `seed-storage`, sandbox S3 workspace |
| `PUBLIC_BASE_URL` | hydra issuer, bkn-safe browser URL, OAuth redirects; must match the browser-visible address exactly |

Ports: `GATEWAY_PORT` for the gateway; middleware debug ports are listed in `.env.example`
(bound to `127.0.0.1` by default).

## Differences from the k8s deployment / limitations

- All middleware is single-replica, no HA; OpenSearch runs with the security plugin disabled (no local certs).
- Single-node Kafka in KRaft mode exposes `PLAINTEXT:9092` (in-container debugging) and `SASL_PLAINTEXT:19092` (services).
- Service configs are **static files** instead of ConfigMap templates: values the charts render via `{{ .Values.* }}` are pre-expanded.
- `bkn-project` keeps its git store in the `bkn-project-git` named volume (matches chart `config.gitStore.rootDir=/data/git`).
- Trace/log export is disabled in the base stack (`otel.trace.enabled=false`); enable per service when using the trace stack.
- Trace evidence/audit Kafka consumption stays off: it needs pre-created topics and injected credentials, out of scope for the base stack.
- `data-migrator` uses the migrations snapshot baked into the image; the k8s installer resolves the same snapshot for its chart version.
- Images are multi-arch; on Apple Silicon, enable Rosetta or use an amd64 host if a variant is missing.
- `docker compose config` is static validation only; real readiness means `up -d` plus a fully healthy `docker compose ps`.

## Relationship to the installer

`deploy.sh` / `preflight.sh` / `onboard.sh` target Kubernetes and are unchanged. This directory is a
parallel single-host path: same images, same service names and config keys, with Helm + k8s replaced by
Compose. Do not run both on one host with overlapping ports (k8s uses NodePort/Ingress, compose uses `GATEWAY_PORT`).
