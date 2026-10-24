"""examples/tasks/hello-seats on real Docker, with the oracle in both seats; no
model APIs are used."""

import asyncio
import hashlib
from pathlib import Path

import pytest

from harbor.models.environment_type import EnvironmentType
from harbor.models.trial.config import (
    EnvironmentConfig,
    Participant,
    TaskConfig,
    TrialConfig,
)
from harbor.trial.trial import Trial

pytestmark = [pytest.mark.runtime, pytest.mark.integration, pytest.mark.asyncio]

TASK = Path(__file__).resolve().parents[2] / "examples" / "tasks" / "hello-seats"
SEATS = ("p1", "p2")


async def test_oracle_in_both_seats_earns_reward_one(tmp_path: Path):
    config = TrialConfig(
        task=TaskConfig(path=TASK),
        # Parallel tests share the Docker daemon's Compose project namespace.
        trial_name=f"seats-{hashlib.sha256(str(tmp_path).encode()).hexdigest()[:12]}",
        trials_dir=tmp_path / "trials",
        participants=[Participant(seat=seat, name="oracle") for seat in SEATS],
        environment=EnvironmentConfig(type=EnvironmentType.DOCKER, delete=True),
    )
    async with asyncio.timeout(600):
        trial = await Trial.create(config)
        result = await trial.run()

    assert result.exception_info is None, result.exception_info
    assert result.finished_at is not None
    assert result.seat_results is not None
    assert {seat.seat: seat.under_test for seat in result.seat_results} == {
        "p1": True,
        "p2": False,
    }
    for seat in result.seat_results:
        assert seat.exception_info is None, (seat.seat, seat.exception_info)
        assert seat.agent_info is not None
        assert seat.agent_info.name == "oracle"
        assert seat.agent_execution is not None
        assert seat.agent_execution.finished_at is not None
    assert result.verifier_result is not None
    assert result.verifier_result.rewards == {"reward": 1.0}
    for seat in SEATS:
        oracle_log = trial.paths.trial_dir / "seats" / seat / "agent" / "oracle.txt"
        assert f"{seat} posts its note" in oracle_log.read_text()
