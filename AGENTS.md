# Styx repository guidance

## Bootstrap

Styx is a Python 3.14 transactional stateful-functions runtime. The deployed system consists of a Coordinator, Worker processes, Kafka ingress and egress, and an S3-compatible snapshot store. Start with `README.md`; use `docs/styx-docs/running-experiments.md` for deployment modes and experiment arguments, and `tests/README.md` for test tiers.

Set up a development environment with:

```bash
python -m pip install -e styx-package/.
python -m pip install -r requirements.txt
```

Use the same checks as continuous integration for changed Python code:

```bash
ruff check .
ruff format --check .
pytest tests/unit/ -q
```

Integration tests require Docker and start Kafka and MinIO through Testcontainers. End-to-end tests require the full Docker Compose stack. Run the narrowest relevant tier before expanding validation.

## Runtime model and invariants

- A Worker in Coordinator metadata is a Python process, not necessarily a container, Pod, or physical server. `WORKER_THREADS` starts multiple Worker processes inside one Worker container. They share an IP and register with different port offsets.
- `n_part` is the number of logical operator partitions. The Coordinator assigns `(operator, partition)` pairs across registered Worker processes. A function call hashes its key to a target partition and follows that partition's assigned Worker address.
- The runtime's local-versus-remote decision compares Worker IP and port. A call marked remote may still remain inside one Pod or physical server. Do not claim cross-machine execution without Pod-to-node and partition-to-Worker evidence.
- Each operator partition has one state owner. Routing a call to an arbitrary Worker is incorrect unless placement, migration, or replication first makes that Worker the owner.
- All Workers and the Coordinator share one logical S3-compatible snapshot store and bucket. The default Compose and Helm deployments use RustFS. Runtime code uses `boto3` and generic `S3_*` settings; MinIO remains an integration-test backend and can also be supplied as a compatible external endpoint.
- Several modules read environment variables at import time. Tests or launchers that change configuration after import must set the environment first or deliberately reload the affected module.

## Experiment and deployment constraints

- Docker Compose computes Worker container scale as `ceil(n_part / WORKER_THREADS)`. Keep logical partitions, registered Worker processes, containers, Pods, and physical nodes distinct in code, documentation, and results.
- `scripts/start_styx_cluster.sh` runs `docker system prune -f --volumes` before startup. This removes unused objects and volumes beyond Styx. Do not run it on a shared or stateful Docker host without explicit approval.
- `run_experiment.sh` supports `docker-compose`, `k8s-minikube`, and `k8s-cluster`, but verify the rendered deployment rather than trusting the requested arguments. The current Kubernetes install path uses static values and does not propagate every experiment setting, including Worker scale, epoch size, compression, and composite-key behavior.
- The scalability and migration runners currently call the Docker Compose lifecycle directly. Do not describe them as Kubernetes-capable until they use the common deployment path and have been exercised there.
- Kubernetes `remote` traffic is not proof of physical network traffic. A cross-node experiment needs physical node identity in Worker registration, topology-aware operator placement, and validation of the final mapping.
- Paper descriptions are historical evidence, not necessarily current runtime configuration. In particular, the paper uses MinIO and an older Python version, while the current default deployment uses RustFS and Python 3.14.

## Validation expectations

- For deployment changes, validate generated manifests, effective environment variables, registered Worker count, Pod placement, and runtime logs. Ready Pods alone do not prove that the requested experiment configuration was applied.
- For performance work, verify that both Styx package and Worker Cython extensions compiled and loaded. The Worker image build can fall back to pure Python after a Cython build failure, so a successful image build is insufficient evidence.
- Preserve result and diagnostic data before tearing down a cluster: client and Kafka output, Coordinator and Worker logs, effective values, image identity, and partition placement.
- Keep changes scoped. Preserve unrelated tracked and untracked files, and stage only reviewed files when committing.
