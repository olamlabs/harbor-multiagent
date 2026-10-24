# Changes from Harbor

This repository is upstream Harbor ([harbor-framework/harbor](https://github.com/harbor-framework/harbor))
at `6099b8cf`, tagged `original-harbor-base-6099b8cf`, plus multi-agent tasks:
several Harbor agents in one environment, each in its own Compose service
(its seat). See [docs-mintlify/tasks/multi-agent.mdx](docs-mintlify/tasks/multi-agent.mdx).

## Harbor files touched

| File | Why |
|---|---|
| `README.md` | Describes this fork and how to install it. Harbor's README is at [harbor-framework/harbor](https://github.com/harbor-framework/harbor). |
| `src/harbor/models/task/config.py` | `SeatConfig` and `TaskConfig.agents` (`[agents.<service>]`), with the seat refusals at task load. |
| `src/harbor/models/task/paths.py` | `seat_instruction_path`: `instructions/<seat>.md`. |
| `src/harbor/models/task/task.py` | Reads each seat's instruction and does not require `instruction.md` for a seat task. |
| `src/harbor/models/trial/config.py` | `Participant` (an `AgentConfig` plus `seat`) and `TrialConfig.participants`, refusing duplicate seats and per-agent settings seats cannot honor. A seat trial's serialized config leaves out `agent`, which no seat runs; reloading restores Harbor's default. |
| `src/harbor/models/job/config.py` | `JobConfig.participants`. |
| `src/harbor/job_plan.py` | Passes the job's participants into each trial config, as it does `user_agent`. |
| `src/harbor/models/job/lock.py` | Records participants in `lock.json` and its equality, so a changed roster is a changed run. |
| `src/harbor/models/trial/result.py` | `SeatResult` (with `ended_by_play_end`, true when the agents under test or the agent timeout ended play) and `TrialResult.seat_results`; seat errors count as recorded errors and seat usage counts in the token and cost totals. |
| `src/harbor/environments/base.py` | `service_upload_file` and `service_upload_dir`, raising the existing unsupported-service error off `main`. |
| `src/harbor/environments/docker/docker.py` | Docker's `service_upload_file` and `service_upload_dir`. |
| `src/harbor/environments/docker/docker_unix.py` | The upload helpers take a service; they were hard-coded to `main`. |
| `src/harbor/environments/dind_compose.py` | `upload_file` and `upload_dir` take a service, as the downloads do, and the transport gains `service_upload_file` and `service_upload_dir`, so every cloud environment that runs Compose through this layer can host multi-agent tasks. |
| `src/harbor/environments/compose_service_ops.py` | `service_upload_file` and `service_upload_dir` on the shared mixin, dispatching to `main` or the compose transport like the downloads. |
| `src/harbor/environments/gke.py` | Its retrying upload overrides take the service argument through. |
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
| `tests/unit/models/test_task_config_seats.py`, `tests/unit/models/test_trial_participants.py`, `tests/unit/environments/test_seat.py`, `tests/unit/environments/test_dind_service_upload.py`, `tests/unit/trial/test_seats.py`, `tests/unit/test_hello_seats_example.py` | Unit tests; none needs Docker. |
| `tests/runtime/test_hello_seats_docker.py` | Runs hello-seats with the oracle in both seats on real Docker. |
