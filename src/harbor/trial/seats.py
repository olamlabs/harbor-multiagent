"""Several Harbor agents in one environment.

A multi-agent task is an ordinary single-environment task whose Compose file
has a service per agent, each named by an ``[agents.<service>]`` table in
task.toml. We call that service the agent's seat. The trial starts the project
as any Compose task, installs every participant's agent into its seat at once,
starts them together under the task's agent timeout, and ends play when every
agent under test has ended. It then collects every seat alike, gives ``main``
the seats' logs at /logs/seats, and runs the shared verifier once with every
container still up.
"""

import asyncio
import contextlib
import copy
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, override

from harbor.agents.base import BaseAgent
from harbor.agents.factory import AgentFactory
from harbor.agents.installed.base import NonZeroAgentExitCodeError
from harbor.environments.seat import SeatEnvironment
from harbor.models.agent.context import AgentContext
from harbor.models.agent.name import AgentName
from harbor.models.task.config import NetworkMode, merge_mcp_servers
from harbor.models.task.task import Task
from harbor.models.task.verifier_mode import (
    VerifierEnvironmentMode,
    resolve_task_verifier_mode,
)
from harbor.models.trial.config import AgentConfig, Participant, TrialConfig
from harbor.models.trial.paths import TrialPaths
from harbor.models.trial.result import ExceptionInfo, SeatResult, TimingInfo
from harbor.tasks.client import TaskDownloadResult
from harbor.trial.errors import AgentSetupTimeoutError, AgentTimeoutError
from harbor.trial.hooks import TrialEvent
from harbor.trial.network_policy import resolve_trial_network_plan
from harbor.trial.single_step import SingleStepTrial
from harbor.utils.trajectory_utils import load_model_usage_from_trajectory


@dataclass
class Seat:
    """One seat's agent, paths and record."""

    name: str
    under_test: bool
    participant: Participant
    agent: BaseAgent
    logger: logging.Logger
    paths: TrialPaths
    instruction: str
    result: SeatResult
    setup_timeout_sec: float
    environment: SeatEnvironment = field(init=False)


def check_launch(config: TrialConfig, task: Task) -> None:
    """Raise ValueError before any environment starts if this trial config
    cannot run the task, naming every problem in one message."""
    seats = set(task.config.agents)
    filled = {participant.seat for participant in config.participants}
    problems = []
    if missing := sorted(seats - filled):
        problems.append(f"seats without a participant: {missing}")
    if unknown := sorted(filled - seats):
        problems.append(f"participants for seats the task does not have: {unknown}")
    if config.agent != AgentConfig():
        problems.append("agent is set; a seat task takes participants")
    if config.user_agent is not None:
        problems.append("user_agent is not supported for seat tasks")
    if resolve_task_verifier_mode(task.config) != VerifierEnvironmentMode.SHARED:
        problems.append("the verifier must run in main (shared mode)")
    plan = resolve_trial_network_plan(
        task.config,
        config.agent,
        config.environment,
        config.verifier,
        None,
        verifier_mode=VerifierEnvironmentMode.SHARED,
    )
    if any(
        policy.network_mode != NetworkMode.PUBLIC
        for policy in (plan.agent_env_baseline, plan.agent_phase, plan.verifier_phase)
    ):
        problems.append("seat tasks support only public network_mode")
    if problems:
        raise ValueError(f"Task {task.name} cannot run: {'; '.join(problems)}.")


@contextlib.asynccontextmanager
async def _seat_tasks() -> AsyncIterator[asyncio.TaskGroup]:
    """A TaskGroup that raises the first failed task's own exception, not an
    exception group, so its type reaches the error policy. A job cancelling
    the trial is raised before any task's exception."""
    try:
        async with asyncio.TaskGroup() as group:
            yield group
    except BaseExceptionGroup as errors:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise asyncio.CancelledError
        raise errors.exceptions[0]


class SeatsTrial(SingleStepTrial):
    """SingleStepTrial with one agent per seat, all in the one environment."""

    seats: list[Seat]

    def __init__(
        self,
        config: TrialConfig,
        *,
        _task: Task | None = None,
        _task_download_result: TaskDownloadResult,
    ):
        if _task is not None:
            check_launch(config, _task)
        super().__init__(
            config, _task=_task, _task_download_result=_task_download_result
        )

    @override
    def _init_logger(self) -> None:
        """Calls Trial._init_logger. Each seat logs through the child
        ``seats.<seat>`` of the trial's logger, and its lines in trial.log
        start with the seat's name."""
        super()._init_logger()
        # Longest logger names first, so a seat named "a.b" is not taken for a
        # child logger of a seat named "a".
        seat_loggers = sorted(
            (
                (f"{self.logger.name}.seats.{name}", name)
                for name in self.task.config.agents
            ),
            key=lambda pair: len(pair[0]),
            reverse=True,
        )

        def name_the_seat(record: logging.LogRecord) -> logging.LogRecord:
            for logger_name, seat in seat_loggers:
                if record.name == logger_name or record.name.startswith(
                    f"{logger_name}."
                ):
                    # A copy, so other handlers keep the unprefixed message.
                    named = copy.copy(record)
                    named.msg = f"[{seat}] {record.msg}"
                    return named
            return record

        if self._log_handler is not None:
            self._log_handler.addFilter(name_the_seat)

    @override
    def _init_agent(self) -> None:
        """Mirrors Trial._init_agent for each seat. Logs go to
        trial/seats/<seat>/agent, MCP servers are the task's plus the seat's
        plus the participant's, and the setup timeout comes from calling
        Trial._compute_agent_setup_timeout_sec. The first agent under test
        is self.agent, which fills agent_info."""
        participants = {p.seat: p for p in self.config.participants}
        self.seats = []
        for name, seat_config in self.task.config.agents.items():
            participant = participants[name]
            paths = TrialPaths(trial_dir=self.paths.trial_dir / "seats" / name)
            paths.agent_dir.mkdir(parents=True, exist_ok=True)
            kwargs: dict[str, Any] = {}
            if participant.name == AgentName.ORACLE.value:
                kwargs = {
                    "task_dir": self.task.task_dir,
                    "trial_paths": paths,
                    "agent_timeout_sec": self._agent_timeout_sec,
                }
            if mcp_servers := merge_mcp_servers(
                self.task.config.environment.mcp_servers,
                seat_config.mcp_servers,
                participant.mcp_servers,
            ):
                kwargs["mcp_servers"] = mcp_servers
            if self._effective_skills_dir:
                kwargs["skills_dir"] = self._effective_skills_dir
            logger = self.logger.getChild(f"seats.{name}")
            agent = AgentFactory.create_agent_from_config(
                participant, logs_dir=paths.agent_dir, logger=logger, **kwargs
            )
            agent.session_id = f"{self.config.trial_name}__{name}"
            agent.context_id = self._id
            seat = Seat(
                name=name,
                under_test=seat_config.under_test,
                participant=participant,
                agent=agent,
                logger=logger,
                paths=paths,
                instruction=self.task.seat_instruction(name),
                result=SeatResult(
                    seat=name,
                    under_test=seat_config.under_test,
                    agent_info=agent.to_agent_info(),
                ),
                setup_timeout_sec=self._compute_agent_setup_timeout_sec(participant),
            )
            self.seats.append(seat)
        self.agent = next(seat.agent for seat in self.seats if seat.under_test)

    @override
    def _init_agent_environment(self) -> None:
        """Calls Trial._init_agent_environment, then makes a SeatEnvironment per
        seat on the project it created."""
        super()._init_agent_environment()
        for seat in self.seats:
            seat.environment = SeatEnvironment(
                self.agent_environment,
                seat.name,
                session_id=f"{self.agent_environment.session_id}__{seat.name}",
                trial_paths=seat.paths,
                logger=seat.logger,
            )

    @override
    def _init_result(self) -> None:
        super()._init_result()
        self.result.seat_results = [seat.result for seat in self.seats]

    @override
    async def _prepare(self) -> None:
        """Mirrors Trial._prepare for one agent per seat: calls
        _validate_agent_capabilities for each seat, starts the project and runs
        its healthcheck, then sets up every seat at once. Any setup failure
        fails the trial."""
        for seat in self.seats:
            self._validate_agent_capabilities(seat.agent, seat.participant)
        await self._setup_agent_environment()
        await self.agent_environment.run_healthcheck()
        setup = TimingInfo(started_at=self._now())
        self.result.agent_setup = setup
        try:
            async with _seat_tasks() as group:
                for seat in self.seats:
                    group.create_task(self._setup_seat(seat))
        finally:
            setup.finished_at = self._now()
        for seat in self.seats:
            seat.result.agent_info = seat.agent.to_agent_info()
        self.result.agent_info = self.agent.to_agent_info()

    async def _setup_seat(self, seat: Seat) -> None:
        """Mirrors Trial._setup_agent for one seat, recording a failure on the
        seat as well as failing the trial."""
        setup = TimingInfo(started_at=self._now())
        seat.result.agent_setup = setup
        try:
            await asyncio.wait_for(
                self._install_seat(seat), timeout=seat.setup_timeout_sec
            )
        except asyncio.TimeoutError as exc:
            timeout = AgentSetupTimeoutError(
                f"Seat {seat.name!r}: agent setup timed out after "
                f"{seat.setup_timeout_sec} seconds"
            )
            seat.result.exception_info = ExceptionInfo.from_exception(timeout)
            raise timeout from exc
        except Exception as exc:
            seat.result.exception_info = ExceptionInfo.from_exception(exc)
            raise
        finally:
            setup.finished_at = self._now()

    async def _install_seat(self, seat: Seat) -> None:
        """Set the seat's Compose user as its default_user, so agents' file
        ownership fixes target that uid, create /logs/agent, then run the
        agent's own setup."""
        environment = seat.environment
        ids = await environment.exec("id -u")
        uid = (ids.stdout or "").strip()
        if ids.return_code != 0 or not uid.isdecimal():
            raise RuntimeError(f"Seat {seat.name!r}: could not resolve its user: {uid}")
        environment.default_user = int(uid)
        logs_dir = self.agent_env_paths.agent_dir
        made = await environment.ensure_dirs([logs_dir])
        if made is not None and made.return_code != 0:
            raise RuntimeError(
                f"Seat {seat.name!r}: could not create {logs_dir}: {made.stdout}"
            )
        with environment.scoped_exec_env(seat.agent.extra_env):
            with self._log_context("agent_setup", environment):
                await seat.agent.setup(environment=environment)

    @override
    async def _run(self) -> None:
        """Mirrors SingleStepTrial._run for one agent per seat: play, collect
        every seat and the task's artifacts, upload trial/seats/ to main's
        /logs/seats/, then call Trial._run_verifier and
        _stop_agent_environment."""
        await self._emit(TrialEvent.AGENT_START)
        execution = TimingInfo(started_at=self._now())
        self.result.agent_execution = execution
        try:
            await self._play()
        finally:
            execution.finished_at = self._now()
            await self._emit(TrialEvent.AGENT_END)
        await self._collect_seats()
        self._record_seat_errors()
        await self._collect_artifacts()
        if not self.config.verifier.disable:
            await self.agent_environment.upload_dir(
                self.paths.trial_dir / "seats",
                (self.agent_env_paths.logs_dir / "seats").as_posix(),
            )
        await self._run_verifier()
        await self._stop_agent_environment()

    @override
    async def _recover_outputs(self) -> None:
        await self._collect_seats()
        await self._collect_artifacts(stop_main_before_sidecars=False)
        await self._stop_agent_environment()

    async def _play(self) -> None:
        """Start every seat at once. Play ends when every agent under test has
        ended or the agent timeout ([agent] timeout_sec) runs out; the runs
        still going are then cancelled, as Trial cancels a single agent at its
        timeout. Nothing in the containers is killed, and they stay up. A seat
        error other than its agent exiting non-zero aborts play and fails the
        trial, as it fails a single-agent trial; aborted seats are not marked
        ended_by_play_end."""
        loop = asyncio.get_running_loop()
        clock = self._agent_timeout_sec
        deadline = None if clock is None else loop.time() + clock
        async with _seat_tasks() as group:
            tasks = {
                seat.name: group.create_task(self._play_seat(seat))
                for seat in self.seats
            }
            under_test = [tasks[seat.name] for seat in self.seats if seat.under_test]
            clock_expired = False
            # Only agents under test hold play open. Waiting on every running
            # seat wakes the loop whenever any seat ends.
            while not all(task.done() for task in under_test):
                running = [task for task in tasks.values() if not task.done()]
                remaining = None if deadline is None else deadline - loop.time()
                done, _ = await asyncio.wait(
                    running,
                    timeout=None if remaining is None else max(remaining, 0),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    clock_expired = True
                    break
            for seat in self.seats:
                if tasks[seat.name].done():
                    continue
                seat.result.ended_by_play_end = True
                if clock_expired:
                    seat.result.exception_info = ExceptionInfo(
                        exception_type=AgentTimeoutError.__name__,
                        exception_message=(
                            f"Agent execution timed out after {clock} seconds"
                        ),
                        exception_traceback="",
                        occurred_at=self._now(),
                    )
                tasks[seat.name].cancel()

    async def _play_seat(self, seat: Seat) -> None:
        """Mirrors Trial._run_agent_phase for one seat, without a clock of its
        own."""
        context = AgentContext()
        execution = TimingInfo(started_at=self._now())
        seat.result.agent_result = context
        seat.result.agent_execution = execution
        try:
            with seat.environment.scoped_exec_env(seat.agent.extra_env):
                with self._log_context("agent", seat.environment):
                    await seat.agent.run(
                        instruction=seat.instruction,
                        environment=seat.environment,
                        context=context,
                    )
        except NonZeroAgentExitCodeError as exc:
            seat.result.exception_info = ExceptionInfo.from_exception(exc)
        except Exception as exc:
            seat.result.exception_info = ExceptionInfo.from_exception(exc)
            raise
        finally:
            execution.finished_at = self._now()

    async def _collect_seats(self) -> None:
        """Mirrors Trial._sync_agent_output for every seat, under test or not."""
        if self._are_agent_logs_downloaded:
            return
        async with _seat_tasks() as group:
            for seat in self.seats:
                group.create_task(self._collect_seat(seat))
        self._are_agent_logs_downloaded = True

    async def _collect_seat(self, seat: Seat) -> None:
        """Mirrors Trial._download_role_logs into trial/seats/<seat>/agent,
        then the trajectory and usage from Trial._sync_agent_output."""
        source_dir = self.agent_env_paths.agent_dir.as_posix()
        participant = seat.participant
        try:
            if participant.include_logs or participant.exclude_logs:
                await seat.environment.download_dir_filtered(
                    source_dir=source_dir,
                    target_dir=seat.paths.agent_dir,
                    include=participant.include_logs or None,
                    exclude=participant.exclude_logs or None,
                )
            else:
                await seat.environment.download_dir(
                    source_dir=source_dir, target_dir=seat.paths.agent_dir
                )
        except Exception as e:
            seat.logger.error(
                f"Failed to download logs to {seat.paths.agent_dir}: {e}",
                exc_info=True,
            )
        context = seat.result.agent_result
        if context is None:
            return
        if context.is_empty():
            seat.agent.populate_context_post_run(context)
        if context.model_usage is None:
            context.model_usage = load_model_usage_from_trajectory(
                seat.agent.logs_dir / "trajectory.json"
            )

    def _record_seat_errors(self) -> None:
        """Put the first seat's error on the trial, as SingleStepTrial._run_agent
        records an agent's timeout or non-zero exit and still verifies. Every
        seat keeps its own in seat_results. The message names the seat, and is
        also written to the trial's exception.txt as Trial writes its own."""
        if self.result.exception_info is not None:
            return
        for seat in self.seats:
            info = seat.result.exception_info
            if info is None:
                continue
            message = f"Seat {seat.name!r}: {info.exception_message}"
            self.result.exception_info = info.model_copy(
                update={"exception_message": message}
            )
            self.paths.exception_message_path.write_text(
                f"{message}\n{info.exception_traceback}"
            )
            return

    @override
    def _secrets_to_scrub(
        self,
        primary_agent: BaseAgent | None = None,
        primary_config: AgentConfig | None = None,
    ) -> set[str]:
        """Trial._secrets_to_scrub for every seat's agent and participant, so
        Trial._scrub_jobs_dir reads the trial dir once."""
        return set().union(
            *(
                super(SeatsTrial, self)._secrets_to_scrub(seat.agent, seat.participant)
                for seat in self.seats
            )
        )
