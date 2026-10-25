"""A seat: one Compose service of the trial's running project, as an environment.

The trial's environment owns the Compose project. A seat environment
gives an unchanged Harbor agent a handle on one other service of that project,
so the agent installs and runs there. Exec, upload and download go through the
project's per-service operations; starting and stopping belong to the project.
"""

import logging
import shlex
from pathlib import Path
from typing import override

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.paths import TrialPaths


class SeatEnvironment(BaseEnvironment):
    """The seat's image, resources, env, user and workdir are its service's, in
    Compose, so its task environment config carries only the task's OS. Any
    environment with per-service exec, upload and download can host it: the
    docker environment and the cloud environments that run Compose."""

    def __init__(
        self,
        project: BaseEnvironment,
        service: str,
        *,
        session_id: str,
        trial_paths: TrialPaths,
        logger: logging.Logger | None = None,
    ):
        self.project = project
        self.service = service
        super().__init__(
            environment_dir=project.environment_dir,
            environment_name=project.environment_name,
            session_id=session_id,
            trial_paths=trial_paths,
            task_env_config=EnvironmentConfig(os=project.task_env_config.os),
            logger=logger,
        )

    @staticmethod
    @override
    def type() -> str:
        return "seat"

    @override
    def _validate_definition(self):
        pass

    @override
    async def start(self, force_build: bool) -> None:
        raise RuntimeError(f"Seat {self.service!r} starts with the trial's project.")

    @override
    async def stop(self, delete: bool):
        raise RuntimeError(f"Seat {self.service!r} stops with the trial's project.")

    @override
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        """Run the command with bash in the seat; agents write bash, and
        service_exec runs ``sh -c``. Without a cwd, the service's Compose
        ``working_dir:`` applies. Without a user, its Compose ``user:`` does, in
        full (uid and gid): default_user, which SeatsTrial sets to that uid for
        agents' ownership fixes, is never sent as ``-u``."""
        with self.project.scoped_output_callback(self._output_callback()):
            return await self.project.service_exec(
                f"exec bash -c {shlex.quote(command)}",
                service=self.service,
                cwd=cwd,
                env=self._merge_env(env),
                timeout_sec=timeout_sec,
                user=user,
            )

    @override
    async def upload_file(self, source_path: Path | str, target_path: str):
        await self.project.service_upload_file(
            source_path, target_path, service=self.service
        )

    @override
    async def upload_dir(self, source_dir: Path | str, target_dir: str):
        await self.project.service_upload_dir(
            source_dir, target_dir, service=self.service
        )

    @override
    async def download_file(self, source_path: str, target_path: Path | str):
        await self.project.service_download_file(
            source_path, target_path, service=self.service
        )

    @override
    async def download_dir(self, source_dir: str, target_dir: Path | str):
        await self.project.service_download_dir(
            source_dir, target_dir, service=self.service
        )
