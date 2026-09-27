# Kubernetes Event.message is deliberately omitted. Emit a fixed safe summary.
def safe_reason:
  if . == "BackOff" or . == "Unhealthy" or . == "FailedScheduling"
     or . == "FailedMount" or . == "Failed" or . == "CrashLoopBackOff"
     or . == "FailedCreate" or . == "DeadlineExceeded"
  then . else "Other" end;

def summary($reason):
  if $reason == "BackOff" or $reason == "CrashLoopBackOff" then "Container repeatedly failed"
  elif $reason == "Unhealthy" then "Health check failed"
  elif $reason == "FailedScheduling" then "Pod scheduling failed"
  elif $reason == "FailedMount" then "Volume mount failed"
  elif $reason == "FailedCreate" then "Workload could not create a resource"
  elif $reason == "DeadlineExceeded" then "Workload exceeded its deadline"
  elif $reason == "Failed" then "Kubernetes operation failed"
  else "Kubernetes warning event" end;

{refreshedAt: $refreshedAt,
 items: [(.items // [])[]
   | select(.type == "Warning")
   | (.reason // "") as $rawReason
   | ($rawReason | safe_reason) as $reason
   | {
       objectKind: (.involvedObject.kind // "Resource"),
       objectName: (.involvedObject.name // "unknown"),
       reason: $reason,
       summary: summary($reason),
       count: (.count // .series.count // 1),
       lastSeen: (.series.lastObservedTime // .lastTimestamp // .eventTime // .metadata.creationTimestamp // "unknown")
     }
   | select(.lastSeen != "unknown")
   | select(.reason != "Other")
   | .] | sort_by(.lastSeen) | reverse | .[:30]}
