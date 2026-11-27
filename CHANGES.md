# Changes from Harbor

This repository is upstream Harbor ([harbor-framework/harbor](https://github.com/harbor-framework/harbor))
merged up to `2ea52267` (v0.26.0 plus 34 commits), plus multi-agent tasks:
several Harbor agents in one environment, each in its own Compose service
(its seat). See [docs-mintlify/tasks/multi-agent.mdx](docs-mintlify/tasks/multi-agent.mdx).

## Harbor files touched

| File | Why |
|---|---|
| `.github/workflows/*.yml` | Harbor runs its CI on Blacksmith runners (`blacksmith-<n>vcpu-<os>` labels), which only a repository with the Blacksmith GitHub App installed can use; this repository does not have it, so those jobs would wait in the queue and never start. Each job runs on the GitHub-hosted runner of the same OS instead (`ubuntu-26.04`, `ubuntu-24.04`, `windows-2025`); the steps are Harbor's. The `agents` runtime suite's test VM gets 4 CPUs and 12 GB, as Harbor's other two runtime suites do, where Harbor gives it 8 and 24: a GitHub-hosted Linux runner has 4 CPUs and 16 GB, and QEMU fails to start a VM larger than its host. The non-runtime suites run one pytest worker per CPU (`-n auto`) where Harbor runs eight: eight workers on four CPUs made the relay tests' 2 to 5 s waits time out; with half the CPUs the Windows suite takes 13 to 16 minutes, so both non-runtime jobs get 30 minutes where Harbor gives 15. Tradeoff: the suites run slower than on the Blacksmith sizes Harbor picked. |
| `README.md` | Describes this fork and how to install it. Harbor's README is at [harbor-framework/harbor](https://github.com/harbor-framework/harbor). |
| `src/harbor/models/task/config.py` | `SeatConfig` and `TaskConfig.agents` (`[agents.<service>]`), with the seat refusals at task load, including a seat named `main`. |
| `src/harbor/models/task/paths.py` | `seat_instruction_path`: `instructions/<seat>.md`. |
| `src/harbor/models/task/task.py` | Reads each seat's instruction and does not require `instruction.md` for a seat task. |
| `src/harbor/models/trial/config.py` | `Participant` (an `AgentConfig` plus `seat`) and `TrialConfig.participants`, refusing duplicate seats and per-agent settings seats cannot honor. |
| `src/harbor/models/job/config.py` | `JobConfig.participants`. |
| `src/harbor/job_plan.py` | Passes the job's participants into each trial config, as it does `user_agent`. |
| `src/harbor/models/job/lock.py` | Records participants in `lock.json` and its equality, so a changed roster is a changed run; the roster compares in seat order, so the same roster listed in another order is the same run. |
| `src/harbor/models/trial/result.py` | `SeatResult` and `TrialResult.seat_results`; seat errors count as recorded errors and seat usage counts in the token and cost totals. The cost total is `None` when a seat that ran a model reported no cost, so an unknown cost does not read as the other seats' sum; a seat without a model costs nothing. |
| `src/harbor/environments/base.py` | `service_upload_file` and `service_upload_dir`, raising the existing unsupported-service error off `main`, and `wait_for_services`, which raises it on an environment without Compose. A seat trial calls it on the agents' services before installing agents, so they are running, and healthy if they have a healthcheck, as Compose judges; it does not wait on every service because a project may have one-shot services that exit. |
| `src/harbor/environments/docker/docker.py` | Docker's `service_upload_file`, `service_upload_dir` and `wait_for_services`. |
| `src/harbor/environments/docker/docker_unix.py` | The upload helpers take a service; they were hard-coded to `main`. |
| `src/harbor/environments/dind_compose.py` | `upload_file` and `upload_dir` take a service, as the downloads do, and the transport gains `service_upload_file` and `service_upload_dir`, so every cloud environment that runs Compose through this layer can host multi-agent tasks. `upload_dir` creates its target directory first (`mkdir -p`, as root), as the local docker environment's tar upload does; Harbor's `docker compose cp` fails when the target's parent does not exist. `wait_for_services` runs `docker compose up --wait` on the named services. |
| `src/harbor/environments/compose_service_ops.py` | `service_upload_file`, `service_upload_dir` and `wait_for_services` on the shared mixin, dispatching to `main` or the compose transport like the downloads. |
| `src/harbor/environments/gke.py`, `tests/unit/environments/test_gke.py` | The Compose (DinD) strategy's retrying `upload_file` and `upload_dir` overrides take the `service` argument through, which uploads into an agent's own service need. Exec environment forwarding also differs as described below; otherwise multi-agent tasks on `gke` have the limits of Harbor's GKE environment. |
| `src/harbor/environments/sail.py`, `tests/unit/environments/test_sail.py`, `pyproject.toml`, `uv.lock` | Harbor's Sail environment picks a Sailbox through the SDK's size labels (`Sailbox.size_for`), which stop at 8 vCPUs, so a task or an `override_cpus` above 8 fails before a Sailbox is created; a multi-agent task that runs a whole Compose project in one VM needs more. Sail's SDK now sizes by vCPU count, 1 to 64, and has dropped `size_for`, so the provider passes the task's CPUs as `vcpu_count` and its memory and storage as whole-GiB limits, and the `sail` extra requires `sail>=0.18.1`. The provider also takes `docker_config_path`, a Docker `config.json`, as the GKE environment does: Sail builds a task's images on its own builders, which have no registry login, so a private image (a compose service's `image:`, a Dockerfile's base, or the task's `docker_image`) failed the build. Each login in the file is passed to Sail's image build as a value sent only with that request. For a Compose task, Harbor's Sail environment has Sail build the project's service images ahead of time into the VM's image (`Image.from_compose`) and only starts them in the VM. Sail's build refuses a service `image:` that carries a digest ("is pinned by digest, but a stored image is found by tag only"), so a project that pins its prebuilt images cannot start. Here a Compose task boots Sail's devbox image, which has Docker, and `docker compose build` and `up` run inside the VM, as in the Daytona, Modal and GKE environments; `docker_config_path` is staged as that Docker's login. Building and pulling need the network, so a Compose VM is created open and gets the task's network policy once its services are up; a single-container VM still has it from creation. The `auths` entries without a credential (written when a credential helper holds it) are skipped, a Docker Hub shorthand image (`org/image`) matches the `docker.io` login, and `docker_config_path` may start with `~`. `max_lifetime_seconds` defaults to 172800 (48 hours), where Harbor's default is no limit, so a Sailbox whose Harbor process dies is still terminated without imposing the previous 24-hour deadline on a 48-hour trial; a caller can pass another value. The Compose resources override reserves no memory for `main`: Harbor's reserves the task's memory, which Docker turns into `memory.low` on main's cgroup, and when that is larger than the VM's memory the kernel reclaims every page from the project's other services, which made a database service time out connections; a Sailbox runs one trial, so the reservation protects nothing. A readiness wait that times out names the last attempt's output. Sail refuses a disk under `MIN_DISK_GIB` (8 GiB) and memory under the least its vCPU count allows (16 GiB), so a task that asks for less, such as a verifier environment with `storage_mb = 4096`, failed at create; the provider raises memory and disk to those minimums, as the SDK's own environment does. Tradeoff: this file differs from Harbor's in sizing, in how a Compose project starts, in its lifetime default and in its memory reservation; a Compose trial builds and pulls on every start, where Sail's build cached the result; Sail's own environment in its SDK (`sail.harbor:SailboxEnvironment`) sizes by vCPU count but has the same digest rule and takes no registry login. |
| `src/harbor/environments/sail.py` (options) | `SailOptions` declares `docker_config_path`. |
| `src/harbor/environments/sail.py`, `tests/unit/environments/test_sail.py` (Compose readiness) | Original Harbor's Sail provider starts Compose through the SDK's `compose_up`, while its Docker provider uses `up --detach --wait`. This provider builds and starts Compose inside the VM, so it uses `up --detach --wait` with the existing build timeout to honor image and Compose healthchecks before agent execution. Native Compose readiness replaces the main-service exec poll; failed waits retain Compose stdout and stderr. Tradeoff: readiness now covers every service, not only main, and an independent one-shot service observed after it exits fails the wait even with exit code 0, matching the Docker provider. A one-shot dependency declared with `service_completed_successfully` can complete successfully. |
| `src/harbor/environments/sail.py`, `src/harbor/environments/sail_exec.py`, `tests/unit/environments/test_sail.py` (execution and cleanup) | Upstream Sail execution buffers output with `Sailbox.run`; this provider always owns a native `Sailbox.exec` handle, buffering without a subscriber or draining both output streams concurrently with one. Cancelling the SDK wait does not stop the command, and native force-cancel signals its process group rather than daemonized descendants; killing a Compose client does not stop its container command. Before returning from cancellation or an execution failure, the provider kills the command's cgroup and waits for it to empty, then cancels and reaps the SDK handle. Direct commands retain Sail's native exec cgroup. Compose execs pause their inner shell before its first fork; the VM moves it into a private child of the service's cgroup before resuming it, preserving service resource limits and excluding PID 1 and entrypoint services. Launch acquisition is shielded so cancellation cannot discard a remote handle still being created. A process-group kill or the seat ancestry sweep cannot contain reparented or new-session children, so a cgroup boundary is necessary. Tradeoff: Compose execution adds a launch barrier and requires the VM's writable cgroup v2 hierarchy; the controller creates each scope directory before launch and removes it after process exit and cancellation cleanup, adding two filesystem API calls per exec; empty cgroups last until the disposable VM is terminated. A slow callback applies pipe backpressure, reconnect replay remains best-effort, and returned output retains the existing 64 MiB-per-stream limit. Upstream shields Sailbox creation with an additional 30-second cleanup deadline; this provider directly awaits the SDK, which reclaims boxes created for cancelled callers. Upstream discards the handle before stopping and logs release failures; this provider retains it until terminate or sleep succeeds and propagates stop errors. Failed-start cleanup preserves the original start error. No custom retries, deadlines or organization-wide Sailbox listing are added. |
| `src/harbor/environments/sail.py`, `tests/unit/environments/test_sail.py` (command deadlines) | Upstream passes a command's `timeout_sec` to Sail, whose deadline can kill the Compose host supervisor before it records the container exec's cgroup. The same existing deadline now runs around the local launch and wait instead, so expiration follows the same cleanup path as caller cancellation and waits for the launch barrier. There is no second command deadline. Tradeoff: if the controller disappears, the command no longer has an independent guest deadline; the Sailbox lifetime still applies. |
| `src/harbor/environments/sail.py`, `tests/unit/environments/test_sail.py` (SSH streaming) | Upstream Harbor's Sail environment refuses `--stream`. This provider enables Sail's native SSH server only when stream mode is requested, persists a non-secret Sailbox ID and task workdir through the existing `stream_handle` contract, and reconnects by ID using the TCP listener and a freshly signed short-lived SSH certificate. Compose handles also identify the project and service; SSH access resolves exactly one running container by its Compose labels, preserves stdin with `docker exec -i`, honors its configured user and group, and resolves its working directory unless the handle explicitly overrides it. This lets Harbor's existing trajectory polling read the selected agent's session logs without changes to upstream core files. The Sailbox's public Ed25519 host key is fetched through the authenticated filesystem API and pinned for each connection: Sail can reuse a public TCP host and port for another box, so a shared first-use host-key cache otherwise rejects a valid new box. Connecting never provisions, resumes, or deletes a Sailbox. Tradeoff: SSH exposes guest port 22 through a public TCP listener authenticated by Sail's organization SSH CA and consumes a raw endpoint; Compose access supports transcripts and container commands but explicitly refuses TCP forwarding, so it does not support the browser editor. Any planned `no_network` policy fails at start because Sail forbids SSH ingress under that policy. Private keys and certificates exist only in a temporary local directory for the connection. |
| `src/harbor/trial/sync_trajectory.py`, `src/harbor/environments/ssh.py`, `tests/unit/trial/test_sync_trajectory.py` | Harbor's stream sync re-archives every native `*.jsonl` session log with `tar -czf` on each 2-second poll and reads the archive through `SSHAccess.run`, whose default deadline is 15 seconds. Session logs that inline screenshots grow to hundreds of megabytes; on a Sailbox a 210 MB log took 15.9 seconds to archive, transfer and convert, and a 472 MB log failed every poll with an empty `TimeoutError`, so the live trajectory stopped updating for the rest of the run while the failure was logged without its exception. The sync now keeps a local copy of the session logs for the duration of the agent run, lists remote sizes with `find ... -exec wc -c`, and reads each log from the end of its copy with `tail -c +N`, keeping only whole lines; a log that shrank is read again and one that disappeared is removed. It converts only when the copy changed or the last conversion did not publish. The read of new bytes passes no deadline (`SSHAccess.run` now accepts `timeout=None`); OpenSSH's keepalive still ends a dead connection. Failures are logged with their exception. Tradeoff: the sync holds a local copy of the session logs on the machine running Harbor until the agent phase ends, and a log rewritten in place to the same or a larger size is not detected; native session logs only append. |
| `src/harbor/trial/regrade.py` | `_compute_agent_setup_timeout_sec` takes the optional `agent_config` argument that `Trial`'s takes for seats. |
| `docs-mintlify/docs.json` | Lists `tasks/multi-agent` in the Tasks group. |
| `src/harbor/cli/trials.py` | `harbor trial start` lists a seat trial's participants (`seat: harness model`, agents under test marked) where it prints `Agent:` for one agent, and preflights each participant instead of the unused default agent. SIGTERM raises `KeyboardInterrupt`, as `harbor run` already makes it do, so a terminated trial takes the cancellation path and writes `result.json` with the logs, tokens and cost collected so far; under Harbor, SIGTERM ends `harbor trial start` without a result. |
| `src/harbor/job.py`, `src/harbor/cli/jobs.py` | A job with `participants` preflights them instead of the unused default agent. |
| `src/harbor/trial/trial.py` | `Trial.create` routes seat tasks to `SeatsTrial`, and refuses `participants` on a task without seats and a regrade of a seat task. `_scrub_jobs_dir` takes its secrets from a new `_secrets_to_scrub`, so a seat trial scrubs once with every seat's secrets. `_secrets_to_scrub`, `_validate_agent_capabilities` and `_compute_agent_setup_timeout_sec` take an optional agent and agent config, defaulting to `self.agent` and `self.config.agent`, so a seat trial passes each seat's instead of swapping trial fields; Harbor's callers are unchanged. A hook every trial registers (`_journal_event`) appends each lifecycle event (`{"event": "agent-start", "timestamp": ...}`) to `trial-events.jsonl` in the trial directory, so a process watching the directory can tell a single-agent or multi-agent trial's phase; Harbor writes no such file. |

Readers of `agent_info` are untouched: a seat trial fills it with its first
agent under test. Readers of `exception_info` are untouched too: a seat
trial puts a seat's error there, as a single-agent trial puts its agent's,
unless the verifier or the trial itself fails after it.

## Exec environment forwarding

Original Harbor puts exec environment values in `-e NAME=VALUE` arguments and,
on some remote transports, in the outer shell command. Process listings and
command diagnostics can expose agent credentials. Exec now passes `-e NAME`
with values in the CLI process environment. Remote providers use their native
environment mapping; transports without one use stdin or a staged shell file
inside a private directory. This keeps values out of both the Docker argv and
the outer transport argv while preserving per-exec overrides and empty values.
Tradeoff: these execution paths differ from Harbor, and providers that need a
staged environment file add an upload and cleanup for each exec. Environment
values remain accessible to processes allowed to inspect process environments;
this change prevents argument disclosure, not access by a privileged host.
Name-only flags share the client's process environment: variables such as
`DOCKER_HOST`, `COMPOSE_*` and `PATH` can also affect that client. Local Docker
keeps its existing credential-store protection by selecting the host's
`DOCKER_CONFIG` through `--config` when a seat overrides that variable.

The upstream files changed for this behavior are:

| Files | Change |
|---|---|
| `src/harbor/environments/docker/docker.py`, `src/harbor/environments/apple_container.py`, `src/harbor/environments/sbx/local.py` | Name-only flags and the subprocess environment. |
| `src/harbor/environments/dind_compose.py` | Name-only Compose exec flags, per-exec environment forwarding, and shared stdin/staged-file transport helpers. |
| `src/harbor/environments/beam.py`, `src/harbor/environments/blaxel.py`, `src/harbor/environments/daytona/environment.py`, `src/harbor/environments/hyperbrowser/environment.py`, `src/harbor/environments/islo.py`, `src/harbor/environments/kernel_docker.py`, `src/harbor/environments/langsmith.py`, `src/harbor/environments/modal.py`, `src/harbor/environments/novita.py`, `src/harbor/environments/prime.py`, `src/harbor/environments/runta/environment.py`, `src/harbor/environments/sail.py`, `src/harbor/environments/sbx/compose.py`, `src/harbor/environments/superserve.py`, `src/harbor/environments/tensorlake.py`, `src/harbor/environments/vercel/compose.py` | Forward exec values through the provider's environment mapping instead of command arguments. |
| `src/harbor/environments/ack.py`, `src/harbor/environments/boxd.py`, `src/harbor/environments/cua_cloud.py`, `src/harbor/environments/cwsandbox.py`, `src/harbor/environments/ec2.py`, `src/harbor/environments/gke.py`, `src/harbor/environments/openshift.py` | Forward values through stdin or private staged files where the remote exec interface has no environment mapping. |
| `src/harbor/environments/aca_sandbox.py`, `src/harbor/environments/runloop.py`, `src/harbor/environments/use_computer.py` | Keep user-switch and desktop exec environment values in staged scripts rather than nested shell arguments. |
| `tests/unit/environments/test_seat.py`, `tests/unit/environments/test_aca_sandbox.py`, `tests/unit/environments/test_prime.py`, `tests/unit/environments/test_runloop.py`, `tests/unit/environments/test_use_computer.py`, `tests/unit/environments/cwsandbox/conftest.py`, `tests/unit/environments/cwsandbox/test_cwsandbox.py` | Cover separate environment transport and retain the existing exec semantics. |
| `tests/unit/environments/test_ack.py`, `tests/unit/environments/test_apple_container.py`, `tests/unit/environments/test_beam.py`, `tests/unit/environments/test_blaxel.py`, `tests/unit/environments/test_boxd.py`, `tests/unit/environments/test_cua_cloud.py`, `tests/unit/environments/test_daytona.py`, `tests/unit/environments/test_docker.py`, `tests/unit/environments/test_ec2.py`, `tests/unit/environments/test_gke.py`, `tests/unit/environments/test_hyperbrowser.py`, `tests/unit/environments/test_islo.py`, `tests/unit/environments/test_kernel.py`, `tests/unit/environments/test_modal.py`, `tests/unit/environments/test_openshift.py`, `tests/unit/environments/test_runta.py`, `tests/unit/environments/test_sail.py`, `tests/unit/environments/test_tensorlake.py`, `tests/unit/environments/cwsandbox/test_cwsandbox_compose.py`, `tests/unit/environments/sbx/test_compose.py`, `tests/unit/environments/sbx/test_local.py`, `tests/unit/test_langsmith_environment.py` | Assert argv contains names rather than values and the separate environment transport preserves values. |

## Collect hook budget

Original Harbor gives every `[[verifier.collect]]` hook its own timeout,
`timeout_sec`, which defaults to 60 seconds. The hooks run one after another,
and a hook that reaches its limit is killed and logged as a warning. Its
evidence is then missing, so the verifier sees an empty or absent receipt. A
probe that drives real services in a sidecar takes as long as those services
take. When several trials share a host, the same probe can go from about 20 s to
over a minute, so a limit the task never chose cuts it off. Raising the limit in
each task only adds more independent deadlines, one per hook, on top of the
verifier's own timeout.

Here an unset hook `timeout_sec` means the hooks of one collection share a
single budget: the verifier timeout that the trial resolves for that step, after
overrides, the maximum and the multiplier. Each hook gets whatever is left of
that budget. A hook's own `timeout_sec` can still shorten its share, but never
extend it. Once the budget is spent, any remaining hooks are skipped with a
warning. Changing the default is the only way to get one backstop. The deadline
lives in the trial, which is the only place that knows the verifier timeout and
runs the hooks. A task can set a value per hook, but it cannot make the hooks
share one.

Tradeoff: a task that relied on the 60-second default now waits up to the
verifier timeout for a stuck hook, and collection plus verification can take up
to twice the verifier timeout. A hook set to 60 seconds explicitly behaves as
before.

The upstream files changed for this behavior are:

| Files | Change |
|---|---|
| `src/harbor/models/task/config.py` | `VerifierCollectConfig.timeout_sec` defaults to `None`. |
| `src/harbor/trial/trial.py` | `_collect_artifacts_phased` sets one deadline from the step's verifier timeout, and `_run_collect_hooks` gives each hook what remains. `_step_verifier_timeout_sec` is annotated `float`, which `_resolve_timeout_sec` always returns. |
| `docs-mintlify/jobs/artifact-collection.mdx` | Describes the shared budget. |
| `tests/unit/test_trial_verifier_artifact_transfer.py`, `tests/unit/models/test_artifact_validation.py`, `tests/unit/test_single_step_trial.py` | Cover the shared budget, an explicit shorter share and a spent budget, and update the old 60-second expectations. |

## Verifier errors after a graded agent error

Harbor records only the first exception on a trial. An agent error whose
policy still grades the trial, such as `AgentTimeoutError` or
`NonZeroAgentExitCodeError`, is recorded first, so when the verifier then
fails (for example a `VerifierTimeoutError` while downloading a large
`/logs/verifier`), Harbor logs the verifier's exception at debug level and
`result.json` keeps the agent's. The trial has no reward and reports only that
the agent timed out, which hides why it was not graded. A multi-step trial does
the same per step.

Here the verifier's exception replaces the agent's on the trial, and on the
step for a multi-step trial, and `exception.txt` holds its traceback. A
second exception field would change the result schema that every reader of
`result.json` parses; replacing the recorded exception keeps the schema and
reports the error that left the trial without a reward.

Tradeoff: a trial whose verifier failed after its agent timed out no longer
names the agent's error in `exception_info`; the agent's timing stays in
`agent_execution`.

| Files | Change |
|---|---|
| `src/harbor/trial/single_step.py` | `_run_verifier` clears the agent's exception before the verifier's propagates, so `Trial.run` records the verifier's. |
| `src/harbor/trial/trial.py` | `_run_step_verifier` records the verifier's exception on the step whether or not the step's agent raised. |
| `tests/unit/test_single_step_trial.py`, `tests/unit/test_multi_step_run_step.py` | Cover a verifier timeout after an agent timeout, in both verifier modes. |

## Verifier logs after a timeout

Original Harbor downloads an unmounted verifier directory only after the test
script returns. The trial's verifier timeout cancels that execution, skipping
the download before the environment is stopped. Partial stdout and metrics are
then lost.

The trial downloads the verifier's logs after timeout cancellation completes and
before raising `VerifierTimeoutError`, using the same mount check, log filters
and download operations as successful verification. This covers shared and
separate verifier environments and multi-step trials. A reward file is retained
as an artifact but is never parsed after a timeout. Download failures are logged
without replacing the timeout. A download already started during verification
is not restarted if the timeout interrupts it. No download deadline is added.

Tradeoff: reporting a verifier timeout waits for artifact download to finish.

| File | Why |
|---|---|
| `src/harbor/verifier/base.py`, `src/harbor/verifier/verifier.py` | Move the existing verifier-directory download into `BaseVerifier.download_logs` so normal verification and timeout recovery use one implementation. Normal verification keeps its existing `DownloadVerifierDirError` classification. |
| `src/harbor/trial/trial.py` | `_verify_with_timeout` downloads logs after cancellation and keeps the typed timeout even if downloading fails. |
| `tests/unit/test_verifier.py`, `tests/unit/test_trial_verifier_separate.py` | Cover mounted and unmounted logs, filters, download failures, cancellation ordering, no restart of an interrupted download and retained unparsed rewards in single-step and multi-step trials with both verifier environment modes. |

## New files

| File | What |
|---|---|
| `src/harbor/environments/desktop_exec.py`, `tests/unit/environments/test_desktop_exec.py` | Private staged exec scripts shared by the desktop environments, keeping environment values and sudo credentials out of shell arguments, with regressions for shell startup and cleanup. |
| `src/harbor/trial/seats.py` | `SeatsTrial`, a `SingleStepTrial` with one agent per seat. It calls Harbor's trial methods where they take what a seat needs, and mirrors, per seat, the ones that hard-code the single agent (`_init_agent`, `_prepare`, `_setup_agent`, `_run_agent_phase`, `_download_role_logs`, `_sync_agent_output`). Upstream Harbor has no multi-seat trial, and the fork previously converted seat trajectories only after play. Each seat's agent run now uses Harbor's existing `sync_trajectory` context with its own environment and agent directory, so transcripts appear during play and polling stops before final collection and conversion. Tradeoff: each active installed agent with service-aware streaming adds an independent SSH connection and periodic transcript reads and conversion. |
| `src/harbor/environments/seat.py` | `SeatEnvironment`: one Compose service of the trial's project as a `BaseEnvironment`, over the per-service operations. Ordinary execs pass no `-u`, so Compose's `user:` applies in full; `default_user` holds that uid for agents' ownership fixes only. Upstream Harbor has no seat environment or seat cleanup. Cleanup signals the seat's exec processes and their descendants before verification. With a shared PID namespace, the seat's entrypoint can have PPID 0 without being PID 1, and a namespace peer's processes are also visible. The scan stays in the seat's container cgroup, including Sail's per-exec child cgroups, and preserves the first process in that cgroup and its descendants. Signals are sent per PID; an exited process is harmless, but a failed signal against the same live process reports the PID, signal error and exit status. This adds reads of Linux procfs but needs no provider API calls. A service-aware project handle enables seat streaming and is copied with the seat service; its workspace is cleared so the provider resolves that service's own working directory. Handles without a service selector keep streaming unsupported. |
| `docs-mintlify/tasks/multi-agent.mdx` | The task format, participants, how a run starts and ends, logs, the verifier, and the limitations. |
| `examples/tasks/hello-seats/`, `examples/configs/features/seats-trial.yaml`, `examples/configs/features/seats-job.yaml` | A runnable two-agent example task and the trial and job configs that run it. |
| `tests/unit/models/test_task_config_seats.py`, `tests/unit/models/test_trial_participants.py`, `tests/unit/environments/test_seat.py`, `tests/unit/environments/test_dind_service_upload.py`, `tests/unit/trial/test_seats.py` | Unit tests; none needs Docker. |
| `tests/runtime/test_hello_seats_docker.py` | Runs hello-seats with the oracle in both seats on real Docker. |
