# Private kind three-replica proof

Run from any directory (Python stdlib + psycopg; Docker, kind, kubectl and Git OpenSSL are required):

    J:/gigglesdb-work/nunya/gd88/live-3e/venv/Scripts/python.exe -I J:/gigglesdb-work/wt/gd88-w1/crates/nunya/harness/k8s/run.py run

This creates `nunya-8c5`, installs pinned Calico v3.31.2, builds/loads the kubernetes image, deploys the repository manifests with harness-only overrides once, imports policy once after all replicas are Running, and runs scenarios 0â€“8 in order. Results and bounded evidence go to `J:/gigglesdb-work/nunya/gd88/live-8c5/`. It automatically tears down in a finally block. A separate recovery command is:

    J:/gigglesdb-work/nunya/gd88/live-3e/venv/Scripts/python.exe -I J:/gigglesdb-work/wt/gd88-w1/crates/nunya/harness/k8s/run.py teardown

Every kubectl/kind command explicitly names `J:/gigglesdb-work/nunya/gd88/kind/kubeconfig`. Mutations check current-context equals `kind-nunya-8c5`. Cluster creation seeds only that private file for the guard. The harness refuses to overwrite an existing kubeconfig. It never uses kind load or the default context. Teardown removes the private file and only owned resources/new image tags; before/after Docker inventories and C: free space are retained.

Overlays: bootstrap policy and test credentials/certs, image/pull policy, replicas=3 (matching the manifest default of 3); repository YAML is read with kubectl client dry-run and remains unchanged. The Postgres fixture uses TLS and synthetic rows. Credentials and large image archives live only under evidence/tmp and are removed on teardown. Scenarios use separate revoke/limit/survival principals; budget checks use bound-parameter `nunya_tokenize`, counting only successes and NY007. Unmet prerequisites are BLOCKED. No product source is changed to make a scenario pass.

Offline checks:

    J:/gigglesdb-work/nunya/gd88/live-3e/venv/Scripts/python.exe -I J:/gigglesdb-work/wt/gd88-w1/crates/nunya/harness/k8s/test_harness.py

The serving term is the unchanged manifest/config default, six seconds. Scenario 0 observes Ready continuously for at least twelve seconds. Scenario 5 compares NY008 with the first observed active acknowledgement; this establishes observation order, not an internal linearization timestamp. Scenario 7 samples the serving Lease after the first successful query and checks the exact confirmed route revision. Network probes originate in the actual Nunya and Postgres pod network namespaces, using binaries from their owned kind node; both must be unable to connect to admin 6480.
