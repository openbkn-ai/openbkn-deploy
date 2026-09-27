# In-cluster live merge for the install-status snapshot (runs in the refresher
# via jq — the kubectl-shell image ships jq but no python).
#
# Inputs (via --slurpfile):
#   $snap — publish-time snapshot /data/install-status.json (expected releases)
#   $work — refresher dump /live/workloads.json (actual deploy/sts state)
# Arg:
#   $now  — UTC ISO timestamp
#
# Output: the snapshot with per-release appVersion overlaid from the actual
# running image tags (versionSource=live), readiness refreshed, and
# liveMergedAt is set to $now while generatedAt remains the publication time.
# Any error aborts jq; the wrapper keeps the previous live file, so failures
# degrade to pre-existing behaviour.

def workload_total($it):
  if $it.kind == "DaemonSet" then ($it.status.desiredNumberScheduled // 0)
  elif $it.kind == "Job" then ($it.spec.completions // 1)
  else ($it.spec.replicas // 1) end;

def workload_ready($it):
  if $it.kind == "DaemonSet" then ($it.status.numberReady // 0)
  elif $it.kind == "Job" then ($it.status.succeeded // 0)
  else ($it.status.readyReplicas // 0) end;

($snap[0]) as $s
| ($work[0]) as $w
| (reduce ($w.items // [])[] as $it ({};
    ($it.metadata // {}) as $md
    | ((($md.annotations // {})["meta.helm.sh/release-name"])
        // (($md.labels // {})["app.kubernetes.io/instance"])
        // $md.name) as $rel
    | if $rel == null then .
      else
        (.[$rel] // {tags: [], ready: 0, total: 0}) as $e
        | ([($it.spec.template.spec.containers // [])[]
            | .image
            | split("/") | last
            | if contains(":") then (split(":") | last) else "latest" end
           ]) as $tags
        | .[$rel] = {
            tags:  (($e.tags + $tags) | unique),
            ready: ($e.ready + workload_ready($it)),
            total: ($e.total + workload_total($it))
          }
      end)) as $actual
| $s
| .releases |= ((. // []) | map(
    . as $r
    | ($actual[$r.name] // null) as $m
    | if $m == null then .
      else
        . + {ready: "\($m.ready)/\($m.total)"}
          + (if ($m.tags | length) > 0
             then {appVersion: ($m.tags | join(",")), versionSource: "live"}
             else {} end)
      end))
# Refresh pod-sourced serviceHealth from live workloads too — otherwise the
# top-line up/degraded count (the dashboard reads serviceHealth[].state) stays
# frozen at publish time, so a service that was merely mid-restart during
# install shows "degraded" forever even after it self-heals. HTTP-sourced
# entries can't be re-probed here (jq only, no curl), so leave those as-is.
# serviceHealth names may carry a "-svc" or "-internal" suffix the workload
# key lacks.
| .serviceHealth |= ((. // []) | map(
    . as $h
    | if ($h.source == "pod")
      then
        (($actual[$h.name])
         // ($actual[($h.name | rtrimstr("-svc"))])
         // ($actual[($h.name | rtrimstr("-internal"))])
         // null) as $m
        | if $m == null then .
          else
            . + {ready: "\($m.ready)/\($m.total)",
                 state: (if ($m.total > 0 and $m.ready >= $m.total) then "up"
                         elif $m.ready > 0 then "degraded"
                         else "down" end)}
          end
      else . end))
| .liveMergedAt = $now
