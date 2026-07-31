# Local cluster deployment plan

## Goal

Deploy the complete Styx experiment stack on self-managed servers. Use the same Kubernetes and Helm path for both a single-node functional deployment and a multi-node experiment deployment. Moving from one node to several nodes should require selecting another values profile and labeling the nodes, not changing application code or switching deployment systems.

The target multi-node topology has three required machines:

- `runtime-a`: Styx Coordinator and one Worker pod containing multiple Worker processes.
- `runtime-b`: one Worker pod containing multiple Worker processes.
- `data`: one Kafka broker, one ZooKeeper instance, one RustFS instance, and their persistent volumes.

An optional fourth `control` machine may run the Kubernetes control plane, the image registry, Prometheus, and Grafana. The experiment load generator is not automatically performance-neutral. It may run there only after verifying that it can sustain the requested input rate without saturation. Otherwise it needs a separate host or reserved resources.

High availability is out of scope. Kafka, ZooKeeper, RustFS, and the Coordinator each use one replica. Loss of the data machine may terminate an experiment and lose its data.

## Important runtime constraint

Styx currently identifies a Worker process by its IP and ports. It does not know the Kubernetes pod or physical node containing that process. A Worker pod may start several Worker processes through `WORKER_THREADS`; all of them share the pod IP and use different port offsets.

Consequently, the existing `in_the_same_network()` decision distinguishes processes, not physical machines. A TCP call to another process in the same pod or to another pod on the same server is considered remote. Kubernetes pod placement alone therefore cannot prove that a function call crossed a physical network link.

Each operator partition has one state owner. A call cannot be sent to an arbitrary remote Worker without moving or replicating that state. A physical cross-node guarantee must be implemented by controlling operator-partition ownership before execution. It cannot be implemented safely by overriding the destination at call time.

## Deployment model

Use a lightweight bare-metal Kubernetes distribution such as k3s for both profiles. The repository should remain distribution-neutral and consume only a kubeconfig, standard node labels, Helm, and `kubectl`.

Define stable role labels:

```text
styx.io/role=runtime
styx.io/runtime-group=a
styx.io/runtime-group=b
styx.io/role=data
styx.io/role=control
```

For a three-machine deployment, `runtime-a` receives `role=runtime,runtime-group=a`, `runtime-b` receives `role=runtime,runtime-group=b`, and the data machine receives `role=data`. The optional control machine receives `role=control`. Kubernetes hostnames remain the authoritative physical-node identities.

Keep a base Helm values file and two overlays:

```text
deploy/values/base.yaml
deploy/values/single-node.yaml
deploy/values/three-node.yaml
```

The single-node profile permits all components on one node and disables the cross-node placement policy. The three-node profile pins data services to the data node, pins the Coordinator to runtime group A, and defines two Worker pools. Pool A selects runtime group A and pool B selects runtime group B; each pool initially contains one multi-process Worker pod. This is more precise than one Deployment with two replicas because it makes the runtime-group identity and node selector explicit. Both profiles use the same images, ports, environment variables, experiment Job, and result collection path.

## Phase 1: make the Helm deployment faithful to experiment configuration

The current Kubernetes path does not propagate all arguments from `run_experiment.sh`. Add values and container environment variables for:

- Worker pools, their replica counts, and their runtime-group selectors.
- `WORKER_THREADS`.
- `SEQUENCE_MAX_SIZE`.
- `ENABLE_COMPRESSION`.
- `USE_COMPOSITE_KEYS`.
- Snapshot interval and any recovery settings used by the selected experiment.

Pass compression and composite-key settings to both Coordinator and Worker processes. Calculate the number of registered Worker processes as the sum of `poolReplicas * WORKER_THREADS` across all Worker pools; validate before graph submission that it satisfies the selected placement plan.

Stop using `latest` for experiment images. Build Coordinator, Worker, and experiment-runner images with an immutable Git commit tag and push them to a registry reachable by every node. Record the image digests with each result.

Add `nodeSelector`, affinity, toleration, resource request, and resource limit values to the Styx chart. Do not hard-code local hostnames in templates.

Acceptance criteria:

- `helm template` shows the same Styx runtime settings requested by the experiment command.
- Coordinator and Worker logs print the effective settings at startup.
- The expected number of Worker processes registers with the Coordinator.

## Phase 2: provide non-replicated data services

Create a local-experiment values section with:

- Kafka `replicaCount=1`.
- Kafka default, offsets-topic, and transaction-state replication factors set to `1`.
- ZooKeeper `replicaCount=1`.
- RustFS `replicaCount=1`.
- One persistent volume for Kafka and one for RustFS, both pinned to the data node.
- Prometheus and Grafana disabled by default or pinned to the optional control node.

Keep Kafka 7.4 and ZooKeeper initially because that is the current tested runtime. Moving Kafka to KRaft is a separate change and is not required for local deployment.

Use one logical RustFS service and one `styx-snapshots` bucket shared by all Workers. Do not create per-Worker RustFS instances. Allow an externally managed S3-compatible endpoint by disabling the bundled RustFS chart and setting `S3_ENDPOINT`.

Replace the hard-coded ZooKeeper namespace with a Helm-generated service address. Add readiness checks that validate the actual S3 bucket and Kafka API rather than only checking that pods are Ready.

For the first implementation, local persistent volumes are acceptable because high availability is out of scope. Document that rescheduling Kafka or RustFS to another machine does not preserve availability. The deployment should fail rather than silently scheduling a data pod onto a runtime node without its volume.

Acceptance criteria:

- The single-node profile can initialize a graph, write snapshots, and recover a Worker from the stored snapshot.
- Kafka and RustFS run exactly once and remain on the labeled data node in the three-node profile.

## Phase 3: register physical topology with the Coordinator

Expose the following values to every Worker pod through the Kubernetes Downward API:

```text
STYX_POD_NAME
STYX_POD_UID
STYX_NODE_NAME
```

The Kubernetes Downward API does not expose arbitrary node labels. Set `STYX_RUNTIME_GROUP` as a normal environment value on each Helm Worker pool. Pin that pool to the matching `styx.io/runtime-group` node label with a node selector. The single-node profile may use one `local` pool.

Extend Worker registration to send a structured descriptor rather than only `(ip, worker_port, protocol_port)`. The descriptor must contain:

- Worker process address and ports.
- Pod name and UID.
- Kubernetes node name.
- Stable runtime group.
- Process index within the pod.

Extend `coordinator.worker_pool.Worker` to retain this metadata. Preserve a clearly named fallback for non-Kubernetes Docker Compose development, where node identity can default to the host name and runtime group can be optional.

Add a Coordinator diagnostic endpoint or structured startup log containing:

```text
operator, partition -> worker id, pod, node, runtime group
```

This mapping is required evidence for claims about cross-machine execution.

Acceptance criteria:

- Multiple Worker processes in one pod register with the same pod and node identity but distinct process IDs and ports.
- Workers on the two runtime machines register with different node names and runtime groups.
- Recovery and re-registration preserve the topology identity and do not confuse another process in the same pod with the failed process.

## Phase 4: add explicit topology-aware partition placement

Retain the current balanced scheduler as the default. Add a separate explicit placement mode for controlled experiments. Do not change all deployments to topology-aware scheduling implicitly.

Represent experiment placement as data, for example:

```yaml
placement:
  mode: explicit
  assignments:
    - operator: caller
      partitions: "*"
      runtimeGroup: a
    - operator: callee
      partitions: "*"
      runtimeGroup: b
```

The final schema must also support per-partition assignment because assigning a whole operator to one group is insufficient for experiments involving calls between partitions of the same operator.

During graph submission, the Coordinator must select only Worker processes in the required runtime group while balancing partitions across the processes inside that group. If a required group has no eligible Worker or insufficient capacity, graph submission must fail before the workload begins.

Add a placement validator for the call edges relevant to each experiment. A two-node cross-machine guarantee is possible only when the declared call relation is two-colorable and every possible caller-target partition pair is assigned to opposite runtime groups. Self-calls, arbitrary data-dependent targets, or odd placement cycles cannot generally satisfy this property without changing the workload or adding state replication. The plan must reject such a requested guarantee rather than report process-level TCP traffic as physical cross-node traffic.

The first cross-node validation workload should be intentionally small: one caller operator in runtime group A synchronously or asynchronously invokes one callee operator in runtime group B for every transaction. After this passes, add explicit placement maps for the paper workloads whose call graphs satisfy the constraint.

Acceptance criteria:

- The Coordinator rejects an incomplete or impossible explicit placement map.
- Every call in the validation workload targets a Worker whose `STYX_NODE_NAME` differs from the caller's.
- Packet or interface counters on both runtime machines confirm traffic over the physical network interface.
- Disabling explicit placement restores the current balanced behavior.

## Phase 5: run experiments inside the cluster

Replace the host-side `kubefwd` dependency with a Kubernetes Job for the benchmark client. The Job uses cluster DNS for Coordinator, Kafka, and RustFS. This keeps the single-node and multi-node execution paths identical and avoids Kafka advertised-listener problems.

Package workload code and TPC-C data generation in an experiment-runner image. Mount a result volume or upload result artifacts after completion. The Job specification must carry all experiment parameters and use a unique run ID.

Split lifecycle operations into explicit commands:

```text
deploy stack
wait and verify topology
run one experiment
collect results and logs
reset experiment state
remove stack
```

Do not reinstall the whole stack for every row of a batch unless the experiment requires clean infrastructure. Reset Kafka topics, snapshot objects, and Coordinator state through a scoped reset operation. Do not use `docker system prune` in the Kubernetes path.

Before cleanup, collect:

- Client request and Kafka output CSV files.
- Coordinator and Worker logs.
- Effective Helm values and rendered manifests.
- Image digests.
- Pod-to-node placement.
- Operator-partition-to-Worker placement.
- Kafka and RustFS status.
- Node CPU, memory, and network-interface measurements.
- Experiment exit status and timestamps.

## Phase 6: validation sequence

Run validation in this order:

1. Render and lint both Helm profiles without a cluster.
2. Deploy the single-node profile and run a short YCSB-T experiment.
3. Verify snapshot creation, Worker restart recovery, result counts, and cleanup on one node.
4. Deploy the same image and base values with the three-node overlay.
5. Verify data-service pinning and Worker topology registration before sending load.
6. Run the two-operator cross-node validation workload and prove physical interface traffic.
7. Run short YCSB-T and TPC-C smoke experiments without an enforced placement map.
8. Add and validate workload-specific placement maps.
9. Run longer performance experiments only after the client is shown not to be the throughput bottleneck.

For every run, validate logical results as well as deployment health. A successful Helm release and Ready pods are not evidence that an experiment used the requested epoch size, process count, placement, or physical network path.

## Expected change areas

- `charts/styx-cluster/`: base dependency configuration and single-replica data services.
- `charts/styx/`: Worker-pool rendering, runtime settings, scheduling constraints, Downward API metadata, and readiness.
- `worker/worker_service.py`: structured topology registration.
- `coordinator/worker_pool.py`: Worker topology metadata and eligible-worker selection.
- `coordinator/coordinator_metadata.py`: explicit operator-partition placement and validation.
- `styx-package/`: placement configuration types if placement is carried with the graph rather than mounted into the Coordinator.
- `scripts/`: deployment, topology verification, experiment Job submission, result collection, and scoped reset.
- `tests/unit/`: registration compatibility, topology-aware scheduling, impossible-plan rejection, and recovery.
- `tests/e2e/`: single-node smoke test and physical-node-aware placement test where a multi-node cluster is available.

## Non-goals

- Kafka, RustFS, Coordinator, or Worker high availability.
- Transparent execution of one state partition on arbitrary Worker replicas.
- Automatic inference of all possible dynamic function-call edges from Python code.
- Treating different Worker processes or pod IPs as proof of different physical machines.
- Reproducing paper-scale performance before the local topology and experiment parameters are observable and validated.
