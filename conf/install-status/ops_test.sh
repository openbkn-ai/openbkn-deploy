#!/usr/bin/env bash
# Regression coverage for the public install-status whitelist.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The refresher reads its explicitly projected ServiceAccount credentials on
# every call. `kubectl` otherwise falls back to localhost and leaves no files
# for nginx to serve. Requests also need a deadline: a running shell alone is
# not evidence of fresh dashboard data.
rg -q 'kube_api()' "${script_dir}/endpoint.yaml"
rg -Fq -- '--server="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS:-443}"' "${script_dir}/endpoint.yaml"
rg -Fq -- '--certificate-authority=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt' "${script_dir}/endpoint.yaml"
rg -Fq -- '--token="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"' "${script_dir}/endpoint.yaml"
for resource in pods 'deploy,sts,ds,jobs' events; do
  rg -q "kube_api get ${resource} -n .* --request-timeout=15s -o json" "${script_dir}/endpoint.yaml"
done

pods='{"items":[{"metadata":{"name":"api-abc","creationTimestamp":"2026-09-01T10:00:00Z","labels":{"app":"api"},"annotations":{"unsafe":"do-not-return"}},"spec":{"nodeName":"private-node","containers":[{"name":"api","image":"registry.internal/api:1.2.3","env":[{"name":"TOKEN","value":"do-not-return"}]}],"initContainers":[{"name":"setup","image":"registry.internal/setup:1.2.3","env":[{"name":"TOKEN","value":"do-not-return"}]}]},"status":{"phase":"Running","startTime":"2026-09-01T10:01:00Z","podIP":"10.0.0.1","conditions":[{"type":"PodScheduled","status":"True","reason":"","message":"private-node"},{"type":"Ready","status":"False","reason":"ContainersNotReady","message":"token=secret"}],"initContainerStatuses":[{"name":"setup","ready":true,"restartCount":1,"state":{"terminated":{"reason":"Completed","exitCode":0,"startedAt":"2026-09-01T10:00:30Z"}}}],"containerStatuses":[{"name":"api","ready":false,"restartCount":4,"state":{"waiting":{"reason":"CrashLoopBackOff","message":"token=secret 10.0.0.1"}},"lastState":{"terminated":{"reason":"Error","exitCode":1,"message":"do-not-return"}}}]}}]}'
workloads='{"items":[{"kind":"Deployment","metadata":{"name":"api","labels":{"app.kubernetes.io/instance":"api"},"annotations":{"meta.helm.sh/release-name":"api"}},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"api"}},"template":{"spec":{"containers":[{"name":"api","image":"registry.internal/api:1.2.3","env":[{"name":"TOKEN","value":"do-not-return"}],"readinessProbe":{"exec":{"command":["cat","/secret"]}}}]}}},"status":{"replicas":2,"readyReplicas":1,"availableReplicas":1,"updatedReplicas":1,"conditions":[{"type":"Progressing","status":"False","reason":"ProgressDeadlineExceeded","message":"secret=hidden"}]}},{"kind":"DaemonSet","metadata":{"name":"node-agent","labels":{},"annotations":{}},"spec":{"selector":{"matchLabels":{"app":"node-agent"}},"template":{"spec":{"containers":[{"name":"agent","image":"secret.registry/agent:4","livenessProbe":{"httpGet":{"path":"/internal-secret"}}}]}}},"status":{"desiredNumberScheduled":3,"currentNumberScheduled":3,"numberReady":2,"numberAvailable":2,"updatedNumberScheduled":3}},{"kind":"Job","metadata":{"name":"batch","labels":{},"annotations":{}},"spec":{"completions":1,"selector":{"matchLabels":{"job":"batch"}},"template":{"spec":{"containers":[{"name":"worker","image":"secret.registry/worker:1"}]}}},"status":{"active":0,"succeeded":0,"failed":1,"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"secret"}]}}]}'
events='{"items":[{"type":"Warning","reason":"BackOff","message":"token=secret 10.0.0.1","count":3,"lastTimestamp":"2026-09-01T12:00:00Z","involvedObject":{"kind":"Pod","name":"api-abc"}},{"type":"Warning","reason":"UnlistedSensitiveReason","message":"secret=hidden","count":1,"lastTimestamp":"2026-09-01T12:02:00Z","involvedObject":{"kind":"Pod","name":"api-def"}}]}'

public_pods="$(jq --arg refreshedAt '2026-09-01T12:00:00Z' -f "${script_dir}/public-pods.jq" <<<"${pods}")"
public_workloads="$(jq --arg refreshedAt '2026-09-01T12:00:00Z' -f "${script_dir}/public-workloads.jq" <<<"${workloads}")"
public_events="$(jq --arg refreshedAt '2026-09-01T12:00:00Z' -f "${script_dir}/public-events.jq" <<<"${events}")"
jq -e '.items[0].metadata.name == "api-abc" and
  .refreshedAt == "2026-09-01T12:00:00Z" and
  .items[0].status.containerStatuses[0].restartCount == 4 and
  .items[0].status.containerStatuses[0].name == "api" and
  .items[0].status.containerStatuses[0].reason == "CrashLoopBackOff" and
  .items[0].status.containerStatuses[0].lastTermination.exitCode == 1 and
  .items[0].metadata.createdAt == "2026-09-01T10:00:00Z" and
  .items[0].status.startTime == "2026-09-01T10:01:00Z" and
  .items[0].status.conditions[1] == {"type":"Ready","status":"False","reason":"ContainersNotReady"} and
  .items[0].status.initContainerStatuses[0].name == "setup" and
  .items[0].spec.containers == [{"name":"api","image":"registry.internal/api:1.2.3"}] and
  .items[0].spec.initContainers == [{"name":"setup","image":"registry.internal/setup:1.2.3"}] and
  ([.. | strings] | all(contains("do-not-return") | not)) and
  ([.. | strings] | all(contains("10.0.0.1") | not)) and
  ([.. | strings] | all(contains("token=secret") | not)) and
  (.items[0] | keys) == ["metadata", "spec", "status"]' <<<"${public_pods}" >/dev/null
jq -e '.items[0].metadata.name == "api" and
  .refreshedAt == "2026-09-01T12:00:00Z" and
  .items[0].status.ready == 1 and
  .items[0].status.available == 1 and
  .items[0].status.updated == 1 and
  .items[0].status.conditions[0].reason == "Rollout deadline exceeded" and
  .items[1].kind == "DaemonSet" and .items[1].status.desired == 3 and .items[1].status.ready == 2 and
  .items[1].spec.containers[0].probes.liveness == "HTTP" and
  .items[2].kind == "Job" and .items[2].status.failed == 1 and
  ([.. | strings] | all(contains("do-not-return") | not)) and
  ([.. | strings] | all(contains("registry.internal") | not)) and
  ([.. | strings] | all(contains("secret.registry") | not)) and
  ([.. | strings] | all(contains("internal-secret") | not)) and
  ([.. | strings] | all(contains("secret=hidden") | not))' <<<"${public_workloads}" >/dev/null
jq -e '.refreshedAt == "2026-09-01T12:00:00Z" and
  (.items | length) == 1 and
  .items[0].reason == "BackOff" and
  .items[0].summary == "Container repeatedly failed" and
  .items[0].objectName == "api-abc" and
  ([.. | strings] | all(contains("token=secret") | not)) and
  ([.. | strings] | all(contains("10.0.0.1") | not))' <<<"${public_events}" >/dev/null

# The dashboard receives a Studio Token through a URL fragment, then every data
# endpoint shares the internal bkn-safe-backed authorization subrequest.
rg -q 'location = /_install_status_auth' "${script_dir}/nginx.conf"
rg -q 'proxy_pass http://127.0.0.1:8081/auth' "${script_dir}/nginx.conf"
for endpoint in install-status.json install-status.pods.json install-status.workloads.json install-status.events.json install-status/logs; do
  rg -q "auth_request /_install_status_auth" <(sed -n "/location = \/${endpoint//\//\\/}/,/^  }/p" "${script_dir}/nginx.conf")
done
rg -q 'location = /install-status/logs' "${script_dir}/nginx.conf"
rg -q 'proxy_pass http://127.0.0.1:8081' "${script_dir}/nginx.conf"
rg -q 'X-Authenticated-User \$install_status_user' "${script_dir}/nginx.conf"
if rg -q 'auth_basic|install-status-log-auth' "${script_dir}/nginx.conf" "${script_dir}/endpoint.yaml"; then
  echo 'legacy Basic Auth configuration unexpectedly remains' >&2
  exit 1
fi

echo 'install-status public whitelist tests passed'
