# Kubernetes policy store (preview)

Multi-replica **serving is unsupported** until GD-0088 stacks 8b (serving
leases and fencing) and 8c (shared sessions/scopes/counters) land. The three
replicas demonstrate policy storage and leader election only. Non-leaders do
not pick up changed revisions until restart in 8a. Do not expose a serving
Service for these replicas.

1. As a cluster administrator, apply `crds.yaml` once.
2. Choose one namespace; apply `rbac.yaml` there (`kubectl -n <namespace> apply -f rbac.yaml`).
3. Prepare a Kubernetes-feature image and edit its name and upstream/bootstrap
   policy in `deployment.yaml`. Create `nunya-credentials` in that namespace
   with `root.key`, `password`, `upstream-ca.crt`, `tls.crt`, `tls.key` and
   `admin-verifier` (a SCRAM verifier). Certificates must cover advertised pod
   IPs and be trusted by management clients; adapt issuance to your cluster.
4. Apply `deployment.yaml` in the same namespace. The init container renders
   plain config strings from the downward API; Nunya has no `env:NAME` syntax.

The CRDs define namespaced policy heads and immutable history objects.
The Role grants policy get/create/update, history get/list/create and
Lease get/update only for `nunya-leader`, plus unscoped Lease create because
Kubernetes cannot name-scope create. Lease create is required on first start. It grants no delete, patch, watch, secret,
pod or cluster rights. The binding grants that Role only to ServiceAccount
`nunya` in the chosen namespace. Secret volume mounting does not require the
application ServiceAccount to read secrets through the API.

ConfigMap and Deployment grant no API permissions. State uses an ephemeral
per-pod emptyDir for this preview. Existing single-replica PVC examples remain
the supported serving deployment. If changing `lease_name`, update the Role's
resourceNames too. Default election timings are 15/10/2 seconds. Expiry uses unchanged Lease observations
measured with a monotonic clock, never the holder's wall time. API calls time out
after five seconds, including shutdown release calls.

Network management requests to followers return 307 with the leader URL; local
Unix administration stays on that pod. Clients refuse redirects by default and
report the leader URL. Explicit `--follow-leader` follows once, only from HTTPS
to HTTPS, using the original TLS roots/`--ca-file`; all advertised endpoints
must be covered by that trust configuration.
