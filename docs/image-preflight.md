# Recreate image preflight

Before an existing release replaces a Recreate Deployment, the installer renders
the exact Helm upgrade using server dry-run and pre-pulls target images on its
current nodes. The old Deployment is unchanged until these checks succeed. Helm
output may contain Secrets: it stays in memory and is never logged or saved.

Short-lived probe Pods preserve the target node selectors, node affinity, tolerations,
runtime class, security contexts, service account and pull secrets. They mount no application volumes,
environment variables or service-account token. Their command is `/`, a directory,
so the application entrypoint cannot execute. Every container must have an image ID.
Probes are deleted on success or failure. Application readiness remains Helm's job.

Probes preserve explicit `Always` and `Never` pull policies. For a complete
`@sha256:` digest, an omitted or `IfNotPresent` policy uses `IfNotPresent`, so an
already cached immutable core image does not join a serial kubelet pull queue
behind a stalled sample image. Mutable tags still use `Always` unless the
operator explicitly selects `Never`. A cache hit proves local image availability;
it does not validate current registry access or credentials. Validate uncached
private pulls separately, or use explicit `Always` when fresh registry validation
is required. `Never` supports preloaded offline images and fails when absent.

`OPENBKN_IMAGE_PREFLIGHT_TIMEOUT` bounds each probe's scheduling/pull wait (default
240 seconds). Authentication, scheduling, insufficient spare resources, a new
service account absent from the current release can fail preflight. Pod affinity
and anti-affinity are explicitly unsupported and fail closed: a probe with different
labels cannot promise identical scheduling semantics. The installer preserves the old release rather than weakening
constraints. This checks existing Recreate Deployment nodes, not future autoscaling
nodes or StatefulSet rollouts. A later rollout can be scheduled on another eligible
node; this check does not prove that node has the image cached. Multi-node rollout
coverage needs separate acceptance before claiming outage prevention across all
schedulers/topologies. First installs use the normal installation path.

The sample service is reconciled even at the same Chart version, avoiding an old
gateway-only skip check. Explicit operator configuration still wins: remove legacy
full GHCR repository overrides when intentionally migrating to SWR. A new Chart
with verified SWR images is required; the installer cannot invent target digests.

Local tests use fake Helm/kubectl responses and real sample Chart rendering. They
do not establish registry availability or cold-cluster performance. Release
acceptance must cover both architectures, GHCR-disabled cold pulls, private registry
credentials, existing-config migration, and failed pulls preserving the old service.
