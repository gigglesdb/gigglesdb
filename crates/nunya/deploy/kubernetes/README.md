# Kubernetes multi-replica deployment

Status: multi-replica serving is UNSUPPORTED. This manifest describes the
intended topology, but the live three-replica proof on kind failed. Run one
replica (`replicas: 1`) for serving until a later release says otherwise.

Failed or unproven in that run:
- Shared sessions across pods FAILED with inconsistent admission: a follower stayed
  refusing with "policy membership requires successful reload"; the leader answered
  "token not valid in this session".
- Cross-pod revocation and per-principal limits failed or were blocked.
- Counter survival across failover failed or was blocked.
- Restart ordering remains unproven.
- Sustained readiness of all three replicas failed (follower serving leases expired).

The intended topology runs three replicas on one node, sharing one 1Gi
ReadWriteOnce PVC mounted at
`/state`. Use local-path or a block CSI storage class with working exclusive
file locks; never NFS, ReadWriteMany or ReadWriteOncePod. Required pod affinity
co-locates replicas on the volume's node (the first pod can self-match).
Only the leader-Lease holder opens the state store; its exclusive file lock is
the fence. Followers use the authenticated peer API. A frozen old leader blocks
failover until killed and its lock released.

This topology is intended to give process failover and rolling restarts, with
no node HA; multi-replica serving remains unproven. Node loss
means an outage until the node or volume returns. RollingUpdate allows one
surge replica on the same node and at most one unavailable replica. Reserve
capacity for that fourth pod. The PDB limits voluntary evictions only.

Use the dedicated `nunya` namespace: the Role grants Lease rights throughout
it. If renaming it, change every explicit namespace, including the RoleBinding
subject namespace and Namespace name, consistently. Route/head/history names
and serving Lease names are dynamic, and serving activation lists Leases;
these rules cannot use resourceNames. Kubernetes cannot name-scope create.
The Role grants no secret, Pod, delete, patch, watch or cluster-scoped rights.
Serving Leases reference their Pod UID without blockOwnerDeletion. Secret
volume mounting does not require API read rights for the ServiceAccount.

## Install

The following are operator instructions; review them against your target cluster.

1. Apply `crds.yaml` as a cluster administrator.
2. Apply `namespace.yaml`.
3. Apply `rbac.yaml`.
4. Create Secrets in `nunya` using files, never committed secret values:

   ```sh
   umask 077
   head -c 32 /dev/urandom > peer.secret
   kubectl -n nunya create secret generic nunya-peer --from-file=secret=peer.secret
   head -c 32 /dev/urandom > root.key
   # Prepare password, upstream-ca.crt, tls.crt, tls.key and admin-verifier
   # (a SCRAM verifier) securely before running this command.
   kubectl -n nunya create secret generic nunya-credentials \
     --from-file=root.key --from-file=password --from-file=upstream-ca.crt \
     --from-file=tls.crt --from-file=tls.key --from-file=admin-verifier
   rm peer.secret root.key
   ```

   Share the same peer Secret (at least 32 random bytes) across all replicas.
   Retain a secure backup of the root key for the persistent state.
5. Apply `networkpolicy.yaml` using a CNI that enforces NetworkPolicy.
   Enforcement depends on the CNI and was not demonstrated on kind's default
   CNI: a non-nunya pod could still reach port 6482.
6. Apply `pdb.yaml`.
7. Edit `deployment.yaml`: supply an image built with `--features kubernetes`,
   the upstream and bootstrap route policy, and the appropriate storage class.
   Keep `replicas: 1` for serving; 3 replicas is for testing the topology only.
   Optionally set the commented nodeSelector to the volume's node. Apply it.

The init container (Alpine 3.21.3) renders the downward API values, including
Pod UID, and SCRAM verifier into plain TOML; Nunya has no general `env:NAME`
expansion. Both containers run as UID/GID 10001 with a read-only root filesystem.
The peer Secret is mounted read-only with mode 0400 and the credentials Secret
with mode 0440; fsGroup gives the non-root process access to Secret volumes and
write access to the PVC.

The ClusterIP Service exposes PostgreSQL 6432 only. Management 6481 uses each
pod's advertised HTTPS address; certificates must cover pod IPs and be trusted
by management clients. Followers return 307 to the elected management URL;
`--follow-leader` follows once from HTTPS to HTTPS with the original roots.
Peers derive that URL's host and use HTTP port 6482 (`state/peer/client.rs`),
so no headless Service is needed. Requests are authenticated but not encrypted
(responses are sealed): keep 6482 restricted to nunya pods in this namespace.
Client 6432 and management 6481 ingress are open by default; narrow their rule
for your clients. Do not add an admin Service: 6480 has unauthenticated reload.
Kubelet probes come from the node; verify your CNI allows those probes.

Readiness `/ready` reflects the serving gate and returns 503 when closed;
liveness `/health` does not treat gate closure as process failure. Shutdown
allows 45 seconds for the 30-second drain. An API outage beyond the serving
Lease term closes every replica's gate. All replicas must serve the same routes.
For an upgrade from pods without serving Leases, restart all pods together.

Per-pod Unix management sockets live at `/state/pods/<pod_name>/nunya.sock`:
`nunya --server unix:/state/pods/<pod_name>/nunya.sock policy status <route>`.
