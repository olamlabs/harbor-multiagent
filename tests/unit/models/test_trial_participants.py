import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from harbor.job_plan import JobPlan
from harbor.models.agent.context import AgentContext
from harbor.models.job.config import JobConfig
from harbor.models.job.lock import TaskLock, TrialLock, VerifierLock
from harbor.models.trial.config import (
    AgentConfig,
    EnvironmentConfig,
    Participant,
    TaskConfig,
    TrialConfig,
)
from harbor.models.trial.result import (
    AgentInfo,
    ExceptionInfo,
    SeatResult,
    TimingInfo,
    TrialResult,
)

ROSTER = [
    {"seat": "maya", "name": "claude-code", "model_name": "anthropic/claude-opus-5"},
    {"seat": "raj", "name": "codex", "model_name": "openai/gpt-5.5"},
]


def _trial_config(**kwargs) -> TrialConfig:
    return TrialConfig(task=TaskConfig(path=Path("task")), **kwargs)


def test_participants_parse_as_agent_configs_with_a_seat():
    config = _trial_config(participants=ROSTER)

    assert [p.seat for p in config.participants] == ["maya", "raj"]
    assert isinstance(config.participants[0], AgentConfig)
    assert config.participants[1].model_name == "openai/gpt-5.5"


def test_participant_needs_a_seat():
    with pytest.raises(ValidationError, match="seat"):
        _trial_config(participants=[{"name": "nop"}])


@pytest.mark.parametrize("model", [TrialConfig, JobConfig])
def test_two_participants_for_one_seat_are_refused(model):
    roster = [{"seat": "maya", "name": "nop"}, {"seat": "maya", "name": "oracle"}]
    kwargs = {"task": TaskConfig(path=Path("task"))} if model is TrialConfig else {}

    with pytest.raises(ValidationError, match=r"Seats \['maya'\] have more than one"):
        model(participants=roster, **kwargs)


@pytest.mark.parametrize(
    "field, value",
    [
        ("override_timeout_sec", 10),
        ("max_timeout_sec", 10),
        ("n_concurrent", 2),
        ("concurrency_group", "pool"),
        ("skills", ["./skills"]),
        ("load_trajectory", "t.json"),
        ("extra_allowed_hosts", ["example.com"]),
    ],
)
def test_per_agent_run_settings_are_refused_on_a_participant(field, value):
    with pytest.raises(ValidationError, match=f"sets {field}"):
        Participant(seat="maya", name="nop", **{field: value})


def test_seat_trial_records_leave_out_the_unused_agent():
    config = _trial_config(participants=ROSTER)

    assert "agent" not in config.model_dump()
    assert "agent" not in json.loads(config.model_dump_json())
    assert "agent" in _trial_config().model_dump()
    assert TrialConfig.model_validate_json(config.model_dump_json()) == config


def test_empty_participants_leave_dumps_unchanged():
    config = _trial_config()

    assert "participants" not in config.model_dump(exclude_none=True)
    assert "participants" not in config.model_dump_json()


def test_trial_config_round_trips_participants():
    config = _trial_config(participants=ROSTER)

    again = TrialConfig.model_validate_json(config.model_dump_json())

    assert again.participants == config.participants


def test_job_plan_passes_participants_to_every_trial():
    job = JobConfig(participants=ROSTER)

    trials = JobPlan.build_trial_configs(
        job,
        [TaskConfig(path=Path("a")), TaskConfig(path=Path("b"))],
        job_id=uuid4(),
    )

    assert [len(trial.participants) for trial in trials] == [2, 2]
    assert trials[0].participants[0].seat == "maya"


def _lock(participants) -> TrialLock:
    return TrialLock(
        task=TaskLock(name="task", type="local", digest=f"sha256:{'a' * 64}"),
        agent=AgentConfig(),
        participants=participants,
        environment=EnvironmentConfig(),
        verifier=VerifierLock(),
    )


def test_trial_lock_tells_rosters_apart():
    roster = [Participant.model_validate(p) for p in ROSTER]
    swapped = [roster[0].model_copy(update={"model_name": "openai/gpt-6-luna"})]

    assert _lock(roster) == _lock([p.model_copy() for p in roster])
    assert _lock(roster) != _lock(swapped + roster[1:])
    assert "participants" not in _lock([]).model_dump_json()


def _result(**kwargs) -> TrialResult:
    config = _trial_config()
    return TrialResult(
        task_name="task",
        trial_name=config.trial_name,
        trial_uri="file:///tmp/trial",
        task_id=config.task.get_task_id(),
        task_checksum="abc",
        config=config,
        agent_info=AgentInfo(name="claude-code", version="1"),
        **kwargs,
    )


def _seat(seat: str, under_test: bool, cost: float, **kwargs) -> SeatResult:
    return SeatResult(
        seat=seat,
        under_test=under_test,
        agent_info=AgentInfo(name="pi", version="1"),
        agent_result=AgentContext(n_input_tokens=10, n_output_tokens=2, cost_usd=cost),
        agent_setup=TimingInfo(started_at=datetime.now(timezone.utc)),
        agent_execution=TimingInfo(started_at=datetime.now(timezone.utc)),
        **kwargs,
    )


def test_seat_results_round_trip_through_result_json():
    result = _result(seat_results=[_seat("maya", True, 0.5), _seat("raj", False, 0.25)])

    again = TrialResult.model_validate_json(result.model_dump_json(indent=4))

    assert again.seat_results == result.seat_results
    assert [(s.seat, s.under_test) for s in again.seat_results or []] == [
        ("maya", True),
        ("raj", False),
    ]


def test_result_without_seats_has_no_seat_results_key():
    assert "seat_results" not in _result().model_dump_json()


def test_token_and_cost_totals_sum_every_seat():
    result = _result(seat_results=[_seat("maya", True, 0.5), _seat("raj", False, 0.25)])

    assert result.compute_token_cost_totals() == (20, None, 4, 0.75)


def test_seat_exceptions_count_as_recorded_exceptions():
    timeout = ExceptionInfo(
        exception_type="AgentTimeoutError",
        exception_message="Agent execution timed out after 1 seconds",
        exception_traceback="",
        occurred_at=datetime.now(timezone.utc),
    )
    result = _result(
        seat_results=[
            _seat("maya", True, 0.5),
            _seat("raj", False, 0.1, exception_info=timeout),
        ]
    )

    assert result.failure_cause is not None
    assert result.failure_cause == timeout.policy.cause
