"""examples/tasks/hello-seats loads, its configs resolve, and its world works."""

import importlib.util
import json
import subprocess
import sys
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import yaml

from harbor.cli import jobs as jobs_cli
from harbor.cli import trials as trials_cli
from harbor.cli.config_sources import load_config_source, merge_config
from harbor.job_plan import JobPlan
from harbor.models.job.config import JobConfig
from harbor.models.task.task import Task
from harbor.models.trial.config import TrialConfig
from harbor.trial.seats import SeatsTrial
from harbor.trial.trial import Trial

REPO = Path(__file__).resolve().parents[2]
TASK = REPO / "examples" / "tasks" / "hello-seats"
CONFIGS = REPO / "examples" / "configs" / "features"


def _load(name: str, cli: Any) -> dict[str, Any]:
    return merge_config(
        {},
        load_config_source(str(CONFIGS / name)),
        append_paths=cli._APPEND_CONFIG_PATHS,
        exclusive_groups=cli._EXCLUSIVE_CONFIG_GROUPS,
    )


def _trial_from_seats_trial_yaml(trials_dir: Path) -> TrialConfig:
    config = TrialConfig.model_validate(_load("seats-trial.yaml", trials_cli))
    config.trials_dir = trials_dir
    return config


def _trial_from_seats_job_yaml(jobs_dir: Path) -> TrialConfig:
    job = JobConfig.model_validate(
        _load("seats-job.yaml", jobs_cli) | {"jobs_dir": jobs_dir}
    )
    [config] = JobPlan.build_trial_configs(job, job.tasks, job_id=uuid4())
    return config


def test_task_loads_with_an_agent_under_test_and_a_supporting_agent():
    task = Task(TASK)

    assert {name: seat.under_test for name, seat in task.config.agents.items()} == {
        "p1": True,
        "p2": False,
    }
    assert task.config.agent.timeout_sec == 600
    assert task.seat_instruction("p1").startswith("You are p1.")
    assert "until a note from p2 appears" in task.seat_instruction("p1")
    assert "until a note from p1 appears" in task.seat_instruction("p2")
    assert task.paths.solve_path.is_file()
    compose = yaml.safe_load((TASK / "environment" / "docker-compose.yaml").read_text())
    assert compose["services"]["main"]["networks"]["play"]["aliases"] == ["world"]
    for seat in ("p1", "p2"):
        service = compose["services"][seat]
        assert service["user"] == "player"
        assert service["command"] == ["sleep", "infinity"]
        assert service["environment"] == {"SEAT": seat}


@pytest.mark.parametrize(
    "build_config", [_trial_from_seats_trial_yaml, _trial_from_seats_job_yaml]
)
async def test_configs_fill_both_seats_with_codex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, build_config
):
    monkeypatch.chdir(REPO)
    monkeypatch.setenv("OPENAI_API_KEY", "placeholder")

    trial = await Trial.create(build_config(tmp_path))

    assert isinstance(trial, SeatsTrial)
    assert trial.task.task_dir.resolve() == TASK
    assert [(seat.name, seat.agent.name()) for seat in trial.seats] == [
        ("p1", "codex"),
        ("p2", "codex"),
    ]
    for seat in trial.seats:
        model = seat.agent.to_agent_info().model_info
        assert model is not None
        assert (model.provider, model.name) == ("openai", "gpt-5.5")
        assert seat.participant.model_api_key_name == "OPENAI_API_KEY"
    assert trial.agent is trial.seats[0].agent


def test_world_stores_notes_and_reports_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    spec = importlib.util.spec_from_file_location(
        "hello_seats_world", TASK / "environment" / "world.py"
    )
    assert spec is not None and spec.loader is not None
    world = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(world)
    notes_path = tmp_path / "notes.jsonl"
    monkeypatch.setattr(world, "NOTES_PATH", notes_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), world.World)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        note = json.dumps({"author": "p1", "text": "hello from p1"}).encode()
        request = urllib.request.Request(
            f"{base}/note", data=note, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 201
        with urllib.request.urlopen(f"{base}/notes") as response:
            assert json.loads(response.read()) == [
                {"author": "p1", "text": "hello from p1"}
            ]
        with urllib.request.urlopen(f"{base}/health") as response:
            assert response.status == 200
    finally:
        server.shutdown()
    assert json.loads(notes_path.read_text()) == {
        "author": "p1",
        "text": "hello from p1",
    }


@pytest.mark.skipif(sys.platform == "win32", reason="requires bash")
@pytest.mark.parametrize("script", ["tests/test.sh", "solution/solve.sh"])
def test_scripts_are_valid_bash(script: str):
    assert subprocess.run(["bash", "-n", str(TASK / script)]).returncode == 0
