# Preserve only fields consumed by the public install-status page.
def safe_reason:
  if . == "OOMKilled" or . == "Error" or . == "Completed"
     or . == "CrashLoopBackOff" or . == "ImagePullBackOff"
     or . == "ErrImagePull" or . == "ContainerCreating"
     or . == "CreateContainerConfigError" or . == "RunContainerError"
  then . else "Other" end;

# A deliberately small, message-free subset of `kubectl describe pod`.
# Condition messages can contain image names, node addresses, mount paths, or
# application output, so retain only a fixed reason-code allowlist.
def safe_condition_reason:
  if . == "ContainersNotReady" or . == "ContainersNotInitialized"
     or . == "Unschedulable" or . == "SchedulingGated"
     or . == "PodFailed" or . == "PodCompleted"
  then . else "Other" end;

def container_status:
  {
    name: (.name // ""),
    ready: (.ready // false), restartCount: (.restartCount // 0),
    state: (if .state.waiting? then "waiting"
            elif .state.terminated? then "terminated"
            elif .state.running? then "running" else "unknown" end),
    reason: ((.state.waiting.reason // .state.terminated.reason // "")
             | if . == "" then "" else safe_reason end),
    exitCode: (.state.terminated.exitCode // null),
    startedAt: (.state.running.startedAt // .state.terminated.startedAt // null),
    lastTermination: (if .lastState.terminated? then {
      reason: ((.lastState.terminated.reason // "Other") | safe_reason),
      exitCode: (.lastState.terminated.exitCode // 0)
    } else null end)
  };

{items: [(.items // [])[] | {
  metadata: {name: (.metadata.name // ""), createdAt: (.metadata.creationTimestamp // null)},
  status: {
    phase: (.status.phase // "Unknown"),
    startTime: (.status.startTime // null),
    conditions: [(.status.conditions // [])[]
      | select(.type == "PodScheduled" or .type == "Initialized" or .type == "Ready" or .type == "ContainersReady")
      | {type: (.type // "Unknown"), status: (.status // "Unknown"), reason: ((.reason // "") | safe_condition_reason)}
    ],
    initContainerStatuses: [(.status.initContainerStatuses // [])[] | container_status],
    containerStatuses: [(.status.containerStatuses // [])[] | container_status]
  }
}], refreshedAt: $refreshedAt}
