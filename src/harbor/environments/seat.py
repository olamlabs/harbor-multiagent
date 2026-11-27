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
from harbor.environments.ssh import StreamHandle
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.paths import TrialPaths

_KILL_EXEC_PROCESSES = r"""
process() {
  stat=$(cat "/proc/$1/stat" 2>/dev/null) || return 1
  set -- ${stat##*) }
  printf '%s %s %s\n' "$2" "$1" "${20}"
}
container_cgroup() {
  group=$(cat "/proc/$1/cgroup" 2>/dev/null) || return 1
  case "${group##*/}" in harbor-exec-*) group=${group%/*} ;; esac
  printf '%s\n' "${group%/}"
}
if ! group=$(container_cgroup "$$"); then
  echo "Could not read the seat's cgroup" >&2
  exit 1
fi
service_pid=
service_start=
for dir in /proc/[0-9]*; do
  pid=${dir#/proc/}
  [ "$(container_cgroup "$pid")" = "$group" ] || continue
  info=$(process "$pid") || continue
  set -- $info
  [ "$1" = 0 ] || continue
  if [ -z "$service_start" ] || [ "$3" -lt "$service_start" ] ||
      { [ "$3" -eq "$service_start" ] && [ "$pid" -lt "$service_pid" ]; }; then
    service_pid=$pid
    service_start=$3
  fi
done
if [ -z "$service_pid" ] || [ "$service_pid" = "$$" ]; then
  echo "Could not identify the service's initial process" >&2
  exit 1
fi
own=" $service_pid "
pid=$$
while [ "$pid" -gt 0 ]; do
  own="$own$pid "
  info=$(process "$pid") || break
  pid=${info%% *}
done
while :; do
  targets=""
  for dir in /proc/[0-9]*; do
    target=${dir#/proc/}
    [ "$(container_cgroup "$target")" = "$group" ] || continue
    info=$(process "$target") || continue
    set -- $info
    case "$2" in Z|X) continue ;; esac
    pid=$target
    while [ "$pid" -gt 1 ]; do
      case "$own" in *" $pid "*) break ;; esac
      parent=${info%% *}
      if [ "$parent" = 0 ]; then targets="$targets $target:$3"; break; fi
      pid=$parent
      info=$(process "$pid") || break
    done
  done
  [ -z "$targets" ] && exit 0
  for entry in $targets; do
    target=${entry%:*}
    started=${entry#*:}
    info=$(process "$target") || continue
    set -- $info
    [ "$3" = "$started" ] || continue
    case "$2" in Z|X) continue ;; esac
    if ! error=$(kill -KILL "$target" 2>&1) && info=$(process "$target"); then
      set -- $info
      [ "$3" = "$started" ] || continue
      case "$2" in Z|X) continue ;; esac
      printf 'PID %s: %s\n' "$target" "$error" >&2
      exit 1
    fi
  done
done
"""


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

    @property
    @override
    def stream_enabled(self) -> bool:
        return (
            self.project.stream_enabled
            and "service" in self.project.stream_handle.reference
        )

    @property
    @override
    def stream_handle(self) -> StreamHandle:
        handle = self.project.stream_handle
        if "service" not in handle.reference:
            return super().stream_handle
        return handle.model_copy(
            update={
                "reference": {**handle.reference, "service": self.service},
                "workspace": None,
            }
        )

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

    async def kill_exec_processes(self) -> None:
        """Kill the seat's exec processes and their descendants before grading.
        The service's initial process and its descendants stay running, even
        if another service owns PID 1 in a shared PID namespace."""
        with self.project.scoped_output_callback(self._output_callback()):
            result = await self.project.service_exec(
                _KILL_EXEC_PROCESSES, service=self.service, user="root"
            )
        if result.return_code != 0:
            raise RuntimeError(
                f"Seat {self.service!r}: could not kill its agent's processes "
                f"(exit {result.return_code}): "
                f"{result.stdout}{result.stderr or ''}"
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
