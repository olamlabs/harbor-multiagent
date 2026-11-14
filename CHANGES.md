# Changes from Harbor

This repository is upstream Harbor ([harbor-framework/harbor](https://github.com/harbor-framework/harbor))
merged up to `7592cfa6` (v0.26.0), plus multi-agent tasks:
several Harbor agents in one environment, each in its own Compose service
(its seat). See [docs-mintlify/tasks/multi-agent.mdx](docs-mintlify/tasks/multi-agent.mdx).

## Harbor files touched

| File | Why |
|---|---|
| `README.md` | Describes this fork and how to install it. Harbor's README is at [harbor-framework/harbor](https://github.com/harbor-framework/harbor). |
| `src/harbor/models/task/config.py` | `SeatConfig` and `TaskConfig.agents` (`[agents.<service>]`), with the seat refusals at task load. |
| `src/harbor/models/task/paths.py` | `seat_instruction_path`: `instructions/<seat>.md`. |
| `src/harbor/models/task/task.py` | Reads each seat's instruction and does not require `instruction.md` for a seat task. |
| `src/harbor/models/trial/config.py` | `Participant` (an `AgentConfig` plus `seat`) and `TrialConfig.participants`, refusing duplicate seats and per-agent settings seats cannot honor. |
| `src/harbor/models/job/config.py` | `JobConfig.participants`. |
| `src/harbor/job_plan.py` | Passes the job's participants into each trial config, as it does `user_agent`. |
| `src/harbor/models/job/lock.py` | Records participants in `lock.json` and its equality, so a changed roster is a changed run. |
| `src/harbor/models/trial/result.py` | `SeatResult` and `TrialResult.seat_results`; seat errors count as recorded errors and seat usage counts in the token and cost totals. |
| `src/harbor/environments/base.py` | `service_upload_file` and `service_upload_dir`, raising the existing unsupported-service error off `main`. |
| `src/harbor/environments/docker/docker.py` | Docker's `service_upload_file` and `service_upload_dir`. |
| `src/harbor/environments/docker/docker_unix.py` | The upload helpers take a service; they were hard-coded to `main`. |
| `src/harbor/environments/dind_compose.py` | `upload_file` and `upload_dir` take a service, as the downloads do, and the transport gains `service_upload_file` and `service_upload_dir`, so every cloud environment that runs Compose through this layer can host multi-agent tasks. |
| `src/harbor/environments/compose_service_ops.py` | `service_upload_file` and `service_upload_dir` on the shared mixin, dispatching to `main` or the compose transport like the downloads. |
| `src/harbor/environments/gke.py` | Its retrying upload overrides take the service argument through. Each exec gets a `CoreV1Api` of its own: `stream()` patches `ApiClient.request` during the handshake, and several agents exec at once, so a shared client made a concurrent REST call (the pod delete at cleanup) fail as a bad handshake. Two launch kwargs the other environments already have in some form: `compose_up_timeout_sec` (EC2 has the same; GKE's `up` was fixed at 120 s, too short for a large world) and `docker_config_path`, a Docker `config.json` staged into the pod so `compose build` can pull private images, as the local docker environment inherits the host's login. In Compose (DinD) mode Harbor reaches the pod over SSH, as the EC2 environment reaches its instance; Harbor's GKE environment runs every command and file transfer through the Kubernetes API server's exec websocket. The `dind` container installs `openssh-server` with `apk` and starts `sshd` (Harbor's `sshd_config`, port 22222) before `dockerd`, with a client key and a host key generated for the trial; Harbor pins the host key for the pod IP and connects as root. The Kubernetes API only creates, reads and deletes the pod. Three failures of the exec path made this necessary: an exec stream passes through a `konnectivity-agent` pod, and GKE deletes those pods as the cluster's nodes scale, which closed agent runs mid-turn; the API server closes an exec websocket on any frame over 32 MiB, which reset uploads of large seat logs; and tar downloads read the stream as text, which replaced non-UTF-8 bytes in every binary file and shifted later tar headers, so directories came back truncated without an error. Compose commands run detached in the dind container and are polled by short SSH commands (`_detached_pod_exec`), as the EC2 environment's `_exec_main_detached` does: a failed poll or output read is followed by a pause, and five in a row fail the command. Uploads are gzipped tar streams (`compress=True`, as Harbor's Prime, SkyPilot and HF environments pass). Tradeoff: Harbor must run where pod IPs are routable (inside the cluster or its VPC); `dind_image` must be Alpine-based with `apk`; direct (single-container) mode still uses the exec path. |
| `src/harbor/environments/sail.py`, `tests/unit/environments/test_sail.py`, `pyproject.toml`, `uv.lock` | Harbor's Sail environment picks a Sailbox through the SDK's size labels (`Sailbox.size_for`), which stop at 8 vCPUs, so a task or an `override_cpus` above 8 fails before a Sailbox is created; a multi-agent task that runs a whole Compose project in one VM needs more. Sail's SDK now sizes by vCPU count, 1 to 64, and has dropped `size_for`, so the provider passes the task's CPUs as `vcpu_count` and its memory and storage as whole-GiB limits, and the `sail` extra requires `sail>=0.18.1`. The provider also takes `docker_config_path`, a Docker `config.json`, as the GKE environment does: Sail builds a task's images on its own builders, which have no registry login, so a private image (a compose service's `image:`, a Dockerfile's base, or the task's `docker_image`) failed the build. Each login in the file is passed to Sail's image build as a value sent only with that request. For a Compose task, Harbor's Sail environment has Sail build the project's service images ahead of time into the VM's image (`Image.from_compose`) and only starts them in the VM. Sail's build refuses a service `image:` that carries a digest ("is pinned by digest, but a stored image is found by tag only"), so a project that pins its prebuilt images cannot start. Here a Compose task boots Sail's devbox image, which has Docker, and `docker compose build` and `up` run inside the VM, as in the Daytona, Modal and GKE environments; `docker_config_path` is staged as that Docker's login. Building and pulling need the network, so a Compose VM is created open and gets the task's network policy once its services are up; a single-container VM still has it from creation. Tradeoff: this file differs from Harbor's in sizing and in how a Compose project starts; a Compose trial builds and pulls on every start, where Sail's build cached the result; Sail's own environment in its SDK (`sail.harbor:SailboxEnvironment`) sizes by vCPU count but has the same digest rule and takes no registry login. |
| `src/harbor/environments/gke.py` (options) | `GKEOptions` declares `docker_config_path` and `compose_up_timeout_sec`, the two kwargs the DinD strategy reads; Harbor's typed options refuse an undeclared kwarg. |
| `src/harbor/environments/sail.py` (options) | `SailOptions` declares `docker_config_path`. |
| `src/harbor/trial/regrade.py` | `_compute_agent_setup_timeout_sec` takes the optional `agent_config` argument that `Trial`'s takes for seats. |
| `docs-mintlify/docs.json` | Lists `tasks/multi-agent` in the Tasks group. |
| `src/harbor/cli/trials.py` | `harbor trial start` lists a seat trial's participants (`seat: harness model`, agents under test marked) where it prints `Agent:` for one agent. |
| `src/harbor/trial/trial.py` | `Trial.create` routes seat tasks to `SeatsTrial`, and refuses `participants` on a task without seats and a regrade of a seat task. `_scrub_jobs_dir` takes its secrets from a new `_secrets_to_scrub`, so a seat trial scrubs once with every seat's secrets. `_secrets_to_scrub`, `_validate_agent_capabilities` and `_compute_agent_setup_timeout_sec` take an optional agent and agent config, defaulting to `self.agent` and `self.config.agent`, so a seat trial passes each seat's instead of swapping trial fields; Harbor's callers are unchanged. |

Readers of `agent_info` are untouched: a seat trial fills it with its first
agent under test. Readers of `exception_info` are untouched too: a seat
trial puts its first seat's error there, as a single-agent trial puts its
agent's.

## New files

| File | What |
|---|---|
| `src/harbor/trial/seats.py` | `SeatsTrial`, a `SingleStepTrial` with one agent per seat. It calls Harbor's trial methods where they take what a seat needs, and mirrors, per seat, the ones that hard-code the single agent (`_init_agent`, `_prepare`, `_setup_agent`, `_run_agent_phase`, `_download_role_logs`, `_sync_agent_output`). |
| `src/harbor/environments/seat.py` | `SeatEnvironment`: one Compose service of the trial's project as a `BaseEnvironment`, over the per-service operations. Ordinary execs pass no `-u`, so Compose's `user:` applies in full; `default_user` holds that uid for agents' ownership fixes only. |
| `docs-mintlify/tasks/multi-agent.mdx` | The task format, participants, how a run starts and ends, logs, the verifier, and the limitations. |
| `examples/tasks/hello-seats/`, `examples/configs/features/seats-trial.yaml`, `examples/configs/features/seats-job.yaml` | A runnable two-agent example task and the trial and job configs that run it. |
| `tests/unit/models/test_task_config_seats.py`, `tests/unit/models/test_trial_participants.py`, `tests/unit/environments/test_seat.py`, `tests/unit/environments/test_dind_service_upload.py`, `tests/unit/trial/test_seats.py` | Unit tests; none needs Docker. |
| `tests/runtime/test_hello_seats_docker.py` | Runs hello-seats with the oracle in both seats on real Docker. |
