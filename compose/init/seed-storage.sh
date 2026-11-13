#!/bin/sh
# 在 oss-gateway-backend 中创建默认对象存储（MinIO / S3 兼容，vendor_type=ECEPH）。
# 幂等：已有任意 storage 则跳过。
#
# 为什么需要：agent-operator-integration 上传技能/文件时按 is_default=true 选取
# storage_id；没有默认存储会报 "default oss gateway storage not found"。
#
# 依赖环境变量：
#   OSS_GATEWAY   oss-gateway-backend 基址（默认 http://oss-gateway-backend:8080）
#   ENDPOINT      客户端（浏览器/CLI）可达的地址，用于生成预签名 URL
#   INTERNAL_ENDPOINT 服务间（operator/sandbox）可达的地址
#   BUCKET        桶名
#   ACCESS_KEY / SECRET_KEY  MinIO 凭据
set -eu

OSS_GATEWAY="${OSS_GATEWAY:-http://oss-gateway-backend:8080}"
ENDPOINT="${ENDPOINT:-http://localhost:9000}"
INTERNAL_ENDPOINT="${INTERNAL_ENDPOINT:-http://minio:9000}"
BUCKET="${BUCKET:-openbkn}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"

echo "waiting for oss-gateway $OSS_GATEWAY/health/ready ..."
i=0
until curl -fsS "$OSS_GATEWAY/health/ready" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "oss-gateway not ready" >&2
    exit 1
  fi
  sleep 2
done

existing=$(curl -fsS "$OSS_GATEWAY/api/v1/storages?page=1&size=1" || true)
case "$existing" in
  *storage_id*)
    echo "storage already exists, skipping"
    exit 0
    ;;
esac

payload=$(cat <<EOF
{
  "storage_name": "minio",
  "vendor_type": "ECEPH",
  "endpoint": "$ENDPOINT",
  "internal_endpoint": "$INTERNAL_ENDPOINT",
  "bucket_name": "$BUCKET",
  "access_key_id": "$ACCESS_KEY",
  "access_key_secret": "$SECRET_KEY",
  "region": "us-east-1",
  "is_default": true
}
EOF
)

resp=$(printf '%s' "$payload" | curl -fsS -X POST "$OSS_GATEWAY/api/v1/storages" \
  -H 'Content-Type: application/json' --data-binary @-)
echo "created default storage: $resp"
