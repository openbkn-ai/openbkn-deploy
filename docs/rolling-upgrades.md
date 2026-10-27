# Service upgrades

The installer calls Helm directly. It no longer creates image-preflight Pods,
performs the extra image dry run, or reads `OPENBKN_IMAGE_PREFLIGHT_TIMEOUT`.
The SWR registry default, explicit private registry overrides and image release
verification are unchanged. Use a published Chart containing the intended update
strategy; removing a script step does not change an older installed Chart.
Both local and repository Chart installations wait for readiness. Repository
installs use the existing `CORE_HELM_TIMEOUT` (default 900 seconds); local installs
retain their existing 600-second timeout. Automatic rollback is not enabled by
this change: assess database compatibility before opting into `--atomic`.

## Rolling workloads

Trace uses `RollingUpdate` with integer `maxUnavailable: 0`, `maxSurge: 1` and
`/health/ready`. A failed pull, unschedulable replacement or failed readiness
leaves the old healthy replica running during the update. This needs capacity
for one additional replica and compatible shared database/runtime state. The
updated Trace Chart also uses a native five-second termination hook on Kubernetes
1.30+ to allow endpoint removal to propagate before the process stops; its
40-second termination grace includes the application's shutdown budget. On
1.30–1.33 the default `PodLifecycleSleepAction` feature gate must remain enabled.
Older Kubernetes omit the hook; uninterrupted successful replacement is not
certified there.
The initial transition from an older Chart cannot retrofit the hook into an
existing Pod. Its successful replacement may still have a brief connection gap;
plan that transition separately. Hard
anti-affinity on a single node may keep the update pending; do not delete the old
Pod to force progress. This is not protection against unrelated node failures.

Keep native Helm errors, Pod status and Events when diagnosing an upgrade:

```sh
kubectl -n openbkn get deployments,pods -o wide
kubectl -n openbkn get events --sort-by=.metadata.creationTimestamp
kubectl -n openbkn describe pod <replacement-pod>
helm -n openbkn status <release>
```

`ImagePullBackOff`/`ErrImagePull` points to the actual pull error in Events;
`FailedScheduling` identifies resource or placement constraints. A running Pod
can still fail readiness: inspect its probe and application diagnostics. Protect
credentials when sharing describe/log output. The installer retains Helm's own
failure output; it does not replace it with a generic image-preflight error.

## Vega exception: task recovery assumes one process

Vega currently remains single-replica `Recreate`. Its Core startup calls
`WorkerManager.Start`, which runs `recoverInterruptedTasks` before listening.
The build worker lists all `running`/`stopping` tasks and marks them failed/stopped;
discover and semantic workers similarly fail all running tasks. These rows have
no live-process ownership check in that path. During overlap a new process can
therefore fail a task still running in the old process. Existing recovery tests
exercise this behavior; a successful idle health check cannot establish safety.

See Core `vega/vega-backend/server/worker/worker_manager.go` and
`{build_task,discover_task,semantic_understanding_task}_worker.go`.
Issue [#69](https://github.com/openbkn-ai/openbkn-deploy/issues/69) stays open for
review of this exception and a separately scoped worker-ownership solution.
This exception does not retain image-preflight for other services.

Until safe overlap is implemented, schedule Vega replacement in a maintenance
window: stop new task submissions, finish/stop active tasks through supported
APIs, verify the target image can be pulled with the deployment's credentials
on eligible nodes, then replace the service. Expect a service gap. Do not
advertise failed-pull continuity or zero downtime for this `Recreate` exception.
Do not change infrastructure StatefulSets, PVC-backed singleton services or
ingress host-port workloads to rolling solely as part of this service change.

## CE/EE and database compatibility

Rolling update and Helm rollback do not undo database DDL. Before switching,
record the source/target immutable image digest, embedded Core commit, applied
migration versions/checksums and enabled Enterprise capabilities. Verify the
pair against an isolated copy or synthetic database before touching business
data. Retain values, Secret references and migration history.

[EE #445](https://github.com/openbkn-ai/openbkn-ee/issues/445) was caused by CE
applying `039` while EE only recognized `038`. EE PR #457 fixes that pairing;
it does not make every CE/EE version interchangeable. Both images must recognize
the applied ledger, and the old process must tolerate any schema change before
overlap is considered safe. Never delete ledger entries or disable validation.

Test both CE→EE and EE→CE on the supported pair, including readiness, existing
records, concurrent task/consumer ownership and Enterprise feature behavior.
`--wait --atomic` can restore Kubernetes release resources on timeout, not SQL
state. Test that rollback target against the resulting schema first. Keep a
compatible image available if a pre-upgrade image cannot read the new schema.

## Legacy probe cleanup

No automatic cleanup touches existing workloads. First inspect only the old
preflight label in the intended namespace:

```sh
kubectl -n openbkn get pods -l openbkn.ai/image-preflight=true
```

After confirming they are abandoned probe Pods, remove only matching names:

```sh
kubectl -n openbkn get pods -l openbkn.ai/image-preflight=true -o name |
while IFS= read -r pod; do
  case "$pod" in
    pod/openbkn-image-preflight-*)
      kubectl -n openbkn delete "$pod" --ignore-not-found --wait=false ;;
  esac
done
```
