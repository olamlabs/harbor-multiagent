# hello-seats

The smallest seat task: two agents in one environment, posting notes to one
world. See [docs-mintlify/tasks/multi-agent.mdx](../../../docs-mintlify/tasks/multi-agent.mdx)
for the rules.

- `main` runs the world, a Python HTTP server on port 8080 that the agents
  reach as `world`. `POST /note` takes JSON `{"author": ..., "text": ...}`,
  `GET /notes` lists the notes, and every note is also written to
  `/logs/world/notes.jsonl`. The verifier runs here too.
- `p1` and `p2` are the agents' services: one small image with bash, curl and
  git, running as the user `player`, with `SEAT` set to the service's name.
- `p1` is the agent under test. It posts `hello from p1`, polls `/notes`
  until p2's note appears, and stops.
- `p2` is a supporting agent (`under_test = false`). It posts `hello from p2`,
  then polls `/notes` until p1's note appears. Play ends when p1's agent ends,
  so p2 may still be running then.
- `solution/solve.sh` does what both instructions ask, so the oracle can fill
  both seats.
- `tests/test.sh` writes reward 1 when p1's note is in
  `/logs/world/notes.jsonl` and each of `/logs/seats/p1/agent/` and
  `/logs/seats/p2/agent/` holds the agent's `trajectory.json` or the oracle's
  `oracle.txt`, and 0 otherwise.

## Run it

On a machine with Docker, from the repository root, with `OPENAI_API_KEY`
set:

```bash
harbor trial start -c examples/configs/features/seats-trial.yaml
```

In a checkout of this repository, run it with `uv run harbor ...`. The config
puts Codex with `openai/gpt-5.5` in both seats.
`examples/configs/features/seats-job.yaml` does the same as a job:
`harbor run -c examples/configs/features/seats-job.yaml`.

To run without an API key, set `name: oracle` on both participants and remove
their `model_name` and `model_api_key_name`.

## What to look for

The trial folder is under `./trials/` and is named `hello-seats__<id>`.

- `seats/p1/agent/` and `seats/p2/agent/`: each agent's logs, for Codex its
  `trajectory.json`.
- `artifacts/logs/world/notes.jsonl`: the notes the world received.
- `verifier/reward.txt`: `1` when the run worked.
- `result.json`: `agent_info` is p1's agent. `seat_results` lists `p1`
  (`under_test: true`) and `p2` (`under_test: false`), each with its own
  `agent_result` (tokens and cost), timings and `exception_info`.
