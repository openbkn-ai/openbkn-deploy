# Expose only status fields needed by the public dashboard. Never expose image
# names, environment, commands, annotations beyond the release key, or messages.
def probe_method($p):
  if $p == null then null
  elif $p.httpGet? then "HTTP"
  elif $p.tcpSocket? then "TCP"
  elif $p.exec? then "exec"
  elif $p.grpc? then "gRPC"
  else "configured" end;

def safe_condition_reason:
  if . == "ProgressDeadlineExceeded" then "Rollout deadline exceeded"
  elif . == "FailedCreate" then "Workload could not create Pods"
  elif . == "BackoffLimitExceeded" then "Job retry limit exceeded"
  elif . == "DeadlineExceeded" then "Job deadline exceeded"
  elif . == "MinimumReplicasUnavailable" then "Minimum replicas unavailable"
  else "Condition reported" end;

{items: [(.items // [])[] as $w
  | ($w.metadata // {}) as $md
  | ($w.spec // {}) as $spec
  | ($w.status // {}) as $status
  | ($w.kind // "Unknown") as $kind
  | {
      kind: $kind,
      metadata: {
        name: ($md.name // ""),
        annotations: {"meta.helm.sh/release-name":
          (($md.annotations // {})["meta.helm.sh/release-name"] // "")},
        labels: {"app.kubernetes.io/instance":
          (($md.labels // {})["app.kubernetes.io/instance"] // "")}
      },
      spec: {
        replicas: (if $kind == "DaemonSet" then ($status.desiredNumberScheduled // 0)
                  elif $kind == "Job" then ($spec.completions // 1)
                  else ($spec.replicas // 1) end),
        desired: (if $kind == "DaemonSet" then ($status.desiredNumberScheduled // 0)
                  elif $kind == "Job" then ($spec.completions // 1)
                  else ($spec.replicas // 1) end),
        containers: [($spec.template.spec.containers // [])[] | {
          name: (.name // ""),
          probes: {
            readiness: probe_method(.readinessProbe),
            liveness: probe_method(.livenessProbe),
            startup: probe_method(.startupProbe)
          }
        }]
      },
      status: {
        desired: (if $kind == "DaemonSet" then ($status.desiredNumberScheduled // 0)
                  elif $kind == "Job" then ($spec.completions // 1)
                  else ($spec.replicas // 1) end),
        actual: (if $kind == "DaemonSet" then ($status.currentNumberScheduled // 0)
                 elif $kind == "Job" then (($status.active // 0) + ($status.succeeded // 0) + ($status.failed // 0))
                 else ($status.replicas // 0) end),
        readyReplicas: (if $kind == "DaemonSet" then ($status.numberReady // 0)
                        elif $kind == "Job" then ($status.succeeded // 0)
                        else ($status.readyReplicas // 0) end),
        ready: (if $kind == "DaemonSet" then ($status.numberReady // 0)
                elif $kind == "Job" then ($status.succeeded // 0)
                else ($status.readyReplicas // 0) end),
        available: (if $kind == "DaemonSet" then ($status.numberAvailable // 0)
                    elif $kind == "Job" then ($status.succeeded // 0)
                    else ($status.availableReplicas // 0) end),
        updated: (if $kind == "DaemonSet" then ($status.updatedNumberScheduled // 0)
                  elif $kind == "Job" then ($status.succeeded // 0)
                  else ($status.updatedReplicas // 0) end),
        failed: ($status.failed // 0),
        active: ($status.active // 0),
        succeeded: ($status.succeeded // 0),
        conditions: [($status.conditions // [])[]
          | select(.status == "False" or ((.type == "Failed" or .type == "FailureTarget") and .status == "True"))
          | {type: (.type // "Unknown"), reason: ((.reason // "") | safe_condition_reason)}
        ][:5]
      }
    }
] , refreshedAt: $refreshedAt}
