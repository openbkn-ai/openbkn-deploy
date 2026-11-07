#!/bin/sh
# 注册 OAuth2 登录入口客户端（等价于 bkn-safe chart 的 client-seed Job）。
# 幂等：已存在则 PATCH，不存在则 POST。内部服务不需要 client，只做 token introspect。
#
# 依赖环境变量：
#   ADMIN              hydra admin URL（默认 http://hydra:4445）
#   WEB_REDIRECTS_JSON openbkn-studio 的回调数组（JSON）
#   WEB_LOGOUTS_JSON   openbkn-studio 的登出回调数组（JSON）
#   CLI_REDIRECT       openbkn-cli 的 loopback 回调
#   CI_SECRET          非空时才创建 ci-runner（client_credentials）
set -eu

ADMIN="${ADMIN:-http://hydra:4445}"
WEB_REDIRECTS_JSON="${WEB_REDIRECTS_JSON:-[\"http://localhost:8000/studio/callback\"]}"
WEB_LOGOUTS_JSON="${WEB_LOGOUTS_JSON:-[\"http://localhost:8000/studio\"]}"
CLI_REDIRECT="${CLI_REDIRECT:-http://127.0.0.1:9010/callback}"
CI_SECRET="${CI_SECRET:-}"

echo "waiting for hydra admin $ADMIN/health/ready ..."
i=0
until curl -fsS "$ADMIN/health/ready" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "hydra admin not ready" >&2
    exit 1
  fi
  sleep 2
done

upsert() {
  # $1=client_id $2=create-json $3=json-patch
  if curl -fsS "$ADMIN/admin/clients/$1" >/dev/null 2>&1; then
    curl -fsS -X PATCH "$ADMIN/admin/clients/$1" \
      -H 'Content-Type: application/json' -d "$3" >/dev/null
    echo "  ~ $1"
  else
    curl -fsS -X POST "$ADMIN/admin/clients" \
      -H 'Content-Type: application/json' -d "$2" >/dev/null
    echo "  + $1"
  fi
}

if [ -n "$CI_SECRET" ]; then
  upsert ci-runner \
    '{"client_id":"ci-runner","client_secret":"'"$CI_SECRET"'","grant_types":["client_credentials"],"token_endpoint_auth_method":"client_secret_post","scope":"authz.read authz.write","audience":["bkn-safe"]}' \
    '[{"op":"add","path":"/client_secret","value":"'"$CI_SECRET"'"},{"op":"add","path":"/grant_types","value":["client_credentials"]},{"op":"add","path":"/token_endpoint_auth_method","value":"client_secret_post"},{"op":"add","path":"/scope","value":"authz.read authz.write"},{"op":"add","path":"/audience","value":["bkn-safe"]}]'
else
  echo "  (skip ci-runner: CI_SECRET not set)"
fi

upsert openbkn-sdk \
  '{"client_id":"openbkn-sdk","grant_types":["urn:ietf:params:oauth:grant-type:device_code","refresh_token"],"response_types":["code"],"token_endpoint_auth_method":"none","scope":"openid offline","audience":["bkn-safe"]}' \
  '[{"op":"add","path":"/grant_types","value":["urn:ietf:params:oauth:grant-type:device_code","refresh_token"]},{"op":"add","path":"/response_types","value":["code"]},{"op":"add","path":"/token_endpoint_auth_method","value":"none"},{"op":"add","path":"/scope","value":"openid offline"},{"op":"add","path":"/audience","value":["bkn-safe"]}]'

upsert openbkn-studio \
  '{"client_id":"openbkn-studio","grant_types":["authorization_code","refresh_token"],"response_types":["code"],"token_endpoint_auth_method":"none","redirect_uris":'"$WEB_REDIRECTS_JSON"',"post_logout_redirect_uris":'"$WEB_LOGOUTS_JSON"',"scope":"openid offline","audience":["bkn-safe"]}' \
  '[{"op":"add","path":"/grant_types","value":["authorization_code","refresh_token"]},{"op":"add","path":"/response_types","value":["code"]},{"op":"add","path":"/token_endpoint_auth_method","value":"none"},{"op":"add","path":"/scope","value":"openid offline"},{"op":"add","path":"/audience","value":["bkn-safe"]}]'

upsert openbkn-cli \
  '{"client_id":"openbkn-cli","grant_types":["authorization_code","refresh_token"],"response_types":["code"],"token_endpoint_auth_method":"none","redirect_uris":["'"$CLI_REDIRECT"'"],"scope":"openid offline all","audience":["bkn-safe"]}' \
  '[{"op":"add","path":"/grant_types","value":["authorization_code","refresh_token"]},{"op":"add","path":"/response_types","value":["code"]},{"op":"add","path":"/token_endpoint_auth_method","value":"none"},{"op":"add","path":"/redirect_uris","value":["'"$CLI_REDIRECT"'"]},{"op":"add","path":"/scope","value":"openid offline all"},{"op":"add","path":"/audience","value":["bkn-safe"]}]'

echo "client seed done"
