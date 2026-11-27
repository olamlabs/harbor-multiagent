"""SeatEnvironment points the docker environment at one Compose service."""

import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from harbor.environments.base import (
    BaseEnvironment,
    ExecResult,
    ServiceOperationsUnsupportedError,
)
from harbor.environments.docker.docker import DockerEnvironment
from harbor.environments.seat import SeatEnvironment
from harbor.environments.ssh import StreamHandle
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.paths import TrialPaths


@pytest.fixture
def project(temp_dir: Path) -> DockerEnvironment:
    env_dir = temp_dir / "environment"
    env_dir.mkdir()
    (env_dir / "docker-compose.yaml").write_text(
        'services:\n  main: {}\n  maya:\n    user: "1000:2000"\n'
    )
    trial_paths = TrialPaths(trial_dir=temp_dir / "trial")
    trial_paths.mkdir()
    env = DockerEnvironment(
        environment_dir=env_dir,
        environment_name="hello",
        session_id="hello__abc__env",
        trial_paths=trial_paths,
        task_env_config=EnvironmentConfig(workdir="/app", env={"TASK": "1"}),
    )
    env._run_docker_compose_command = AsyncMock(
        return_value=ExecResult(stdout="", stderr="", return_code=0)
    )
    return env


@pytest.fixture
def seat(project: DockerEnvironment, temp_dir: Path) -> SeatEnvironment:
    return SeatEnvironment(
        project,
        "maya",
        session_id="hello__abc__env__maya",
        trial_paths=TrialPaths(trial_dir=temp_dir / "trial" / "seats" / "maya"),
    )


def _commands(project: DockerEnvironment) -> list[list[str]]:
    mock = project._run_docker_compose_command
    assert isinstance(mock, AsyncMock)
    return [call.args[0] for call in mock.call_args_list]


async def test_exec_wraps_in_bash_and_keeps_the_service_user_and_workdir(project, seat):
    await seat.exec("set -o pipefail; echo hi | cat")

    assert _commands(project)[-1] == [
        "exec",
        "maya",
        "sh",
        "-c",
        "exec bash -c " + shlex.quote("set -o pipefail; echo hi | cat"),
    ]


async def test_exec_passes_explicit_user_cwd_and_scoped_env(project, seat):
    with seat.scoped_exec_env({"INVOCATION_ID": "tok"}):
        await seat.exec("whoami", cwd="/home/maya", env={"A": "1"}, user="root")

    command = _commands(project)[-1]
    assert command[: command.index("maya")] == [
        "exec",
        "-w",
        "/home/maya",
        "-e",
        "A",
        "-e",
        "INVOCATION_ID",
        "-u",
        "root",
    ]
    assert project._run_docker_compose_command.call_args.kwargs["exec_env"] == {
        "A": "1",
        "INVOCATION_ID": "tok",
    }


async def test_compose_identity_applies_in_full_and_main_settings_do_not(project, seat):
    project.default_user = "agent"
    seat.default_user = 1000

    await seat.exec("id")
    await seat.exec("apt-get install -y curl", user="root")

    ordinary, root = _commands(project)[-2:]
    assert ordinary[:2] == ["exec", "maya"]
    assert "-u" not in ordinary
    assert root[:4] == ["exec", "-u", "root", "maya"]
    assert "TASK=1" not in ordinary and "/app" not in ordinary


async def test_seat_reports_what_agents_read(seat, temp_dir):
    assert seat.default_user is None
    assert seat.capabilities.mounted is False
    assert seat.capabilities.stream is False
    assert seat.task_env_config.workdir is None
    assert seat.trial_paths.agent_dir == temp_dir / "trial" / "seats" / "maya" / "agent"
    assert seat.session_id == "hello__abc__env__maya"
    assert seat.os == seat.project.os


def test_streaming_requires_project_stream_and_service_aware_handle(project, seat):
    assert seat.stream_enabled is False
    project._stream_handle = StreamHandle(
        provider="sail", reference={"sailbox_id": "sb_test", "service": "main"}
    )
    assert seat.stream_enabled is False

    project._stream = True
    assert seat.stream_enabled is True

    project._stream = False
    assert seat.stream_enabled is False


def test_stream_handle_scopes_a_copy_to_each_seat(project, seat, temp_dir):
    handle = StreamHandle(
        provider="sail",
        reference={"sailbox_id": "sb_test", "service": "main"},
        workspace="/workspace",
    )
    project._stream_handle = handle
    other = SeatEnvironment(
        project,
        "raj",
        session_id="hello__abc__env__raj",
        trial_paths=TrialPaths(trial_dir=temp_dir / "trial" / "seats" / "raj"),
    )

    assert seat.stream_handle == handle.model_copy(
        update={
            "reference": {"sailbox_id": "sb_test", "service": "maya"},
            "workspace": None,
        }
    )
    assert other.stream_handle.reference["service"] == "raj"
    assert other.stream_handle.workspace is None
    assert project.stream_handle is handle
    assert handle.reference["service"] == "main"
    assert handle.workspace == "/workspace"


async def test_streaming_keeps_service_user_and_workdir_instead_of_main(project, seat):
    project._stream = True
    project.default_user = "root"
    project._stream_handle = StreamHandle(
        provider="sail",
        reference={"sailbox_id": "sb_test", "service": "main"},
        workspace="/main-workdir",
    )
    seat.default_user = 1000

    await seat.exec("pwd; id")

    command = _commands(project)[-1]
    assert command[:2] == ["exec", "maya"]
    assert "-u" not in command
    assert "-w" not in command
    assert seat.stream_enabled is True
    assert seat.stream_handle.workspace is None
    assert seat.stream_handle.reference["service"] == "maya"


def test_streaming_stays_unsupported_without_a_service_selector(project, seat):
    project._stream = True
    project._stream_handle = StreamHandle(
        provider="docker", reference={"container_name": "main"}
    )

    assert seat.stream_enabled is False
    with pytest.raises(NotImplementedError, match="seat does not provide SSH access"):
        _ = seat.stream_handle


async def test_uploads_target_the_seat(project, seat, temp_dir):
    source = temp_dir / "config.json"
    source.write_text("{}")

    await seat.upload_file(source, "/home/maya/.config.json")
    await seat.upload_dir(temp_dir, "/opt/agent")

    commands = _commands(project)
    assert [c for c in commands if c[0] == "cp"] == [
        ["cp", str(source), "maya:/home/maya/.config.json"],
        ["cp", f"{temp_dir}/.", "maya:/opt/agent"],
    ]
    assert all(c[1] == "maya" for c in commands if c[0] == "exec")


async def test_upload_tar_fallback_targets_the_seat(project, seat, temp_dir):
    source = temp_dir / "config.json"
    source.write_text("{}")

    async def runner(command, check=True, **_):
        if command[0] == "cp":
            raise RuntimeError("cp unsupported")
        return ExecResult(return_code=1 if "test" in command else 0)

    project._run_docker_compose_command = AsyncMock(side_effect=runner)

    await seat.upload_file(source, "/home/maya/config.json")

    tar_steps = [c for c in _commands(project) if c[0] == "exec"]
    assert all(c[:5] == ["exec", "-T", "-u", "root", "maya"] for c in tar_steps)
    assert tar_steps[-1][5:] == ["tar", "-xf", "-", "-C", "/home/maya"]


async def test_downloads_target_the_seat(project, seat, temp_dir):
    await seat.download_file("/logs/agent/a.txt", temp_dir / "a.txt")
    await seat.download_dir("/logs/agent", temp_dir / "agent")

    assert _commands(project)[-2:] == [
        ["cp", "maya:/logs/agent/a.txt", str(temp_dir / "a.txt")],
        ["cp", "maya:/logs/agent/.", str(temp_dir / "agent")],
    ]


async def test_inherited_helpers_run_in_the_seat(project, seat):
    await seat.is_dir("/logs/agent")
    await seat.ensure_dirs(["/logs/agent"])

    is_dir, ensure = _commands(project)[-2:]
    assert is_dir[1] == "maya" and "test -d /logs/agent" in is_dir[-1]
    assert ensure[1:4] == ["-u", "root", "maya"]
    assert "mkdir -p /logs/agent && chmod 777 /logs/agent" in ensure[-1]


async def test_seat_output_streams_through_its_own_callbacks(project, seat):
    seen: list[str] = []

    async def runner(command, on_output=None, **_):
        assert on_output is not None
        await on_output("hello\n", "stdout")
        return ExecResult(return_code=0)

    async def collect(text, stream):
        seen.append(text)

    project._run_docker_compose_command = AsyncMock(side_effect=runner)
    with seat.scoped_output_callback(collect):
        await seat.exec("echo hello")

    assert seen == ["hello\n"]


@pytest.mark.skipif(sys.platform == "win32", reason="Requires POSIX sh")
@pytest.mark.parametrize("vanishes", [True, False], ids=["vanished", "survives"])
async def test_seat_cleanup_handles_signal_races(project, seat, temp_dir, vanishes):
    proc = temp_dir / "proc"
    for pid, parent, started, group in [
        (1, 0, 1, "/../display"),
        (9000000, 0, 2, "/"),
        (9000001, 9000000, 3, "/"),
        (9000002, 0, 4, "/harbor-exec-agent"),
        (9000003, 0, 5, "/harbor-exec-agent"),
        (9000004, 0, 6, "/../display"),
    ]:
        path = proc / str(pid)
        path.mkdir(parents=True)
        (path / "stat").write_text(
            f"{pid} (sleep) S {parent}" + " 0" * 17 + f" {started}\n"
        )
        (path / "cgroup").write_text(f"0::{group}\n")

    remove = "rm -r proc/9000002" if vanishes else ":"
    prelude = f"""
mkdir "proc/$$"
printf '%s\\n' '0::/harbor-exec-cleanup' > "proc/$$/cgroup"
printf '%s\\n' "$$ (sh) S 0{" 0" * 17} 7" > "proc/$$/stat"
kill() {{
  case "$2" in
    9000002)
      {remove}
      echo 'kill: process not found or permission denied' >&2
      return 1 ;;
    9000003) rm -r proc/9000003 ;;
    *) echo "Unexpected cleanup target: $2" >&2; return 1 ;;
  esac
}}
"""

    async def runner(command, **_):
        assert command[:6] == ["exec", "-u", "root", "maya", "sh", "-c"]
        result = subprocess.run(
            ["sh", "-ec", prelude + command[-1].replace("/proc/", "proc/")],
            cwd=temp_dir,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return ExecResult(
            stdout=result.stdout, stderr=result.stderr, return_code=result.returncode
        )

    project._run_docker_compose_command = AsyncMock(side_effect=runner)
    if vanishes:
        await seat.kill_exec_processes()
        assert not (proc / "9000003").exists()
    else:
        with pytest.raises(RuntimeError, match=r"\(exit 1\): PID 9000002: kill:"):
            await seat.kill_exec_processes()
    assert all((proc / str(pid)).exists() for pid in (9000000, 9000001, 9000004))


async def test_seat_refuses_start_and_stop(seat):
    with pytest.raises(RuntimeError, match="starts with the trial's project"):
        await seat.start(force_build=False)
    with pytest.raises(RuntimeError, match="stops with the trial's project"):
        await seat.stop(delete=True)


async def test_base_environment_refuses_service_uploads(temp_dir):
    class Plain(BaseEnvironment):
        @staticmethod
        def type() -> str:
            return "plain"

        def _validate_definition(self):
            pass

        async def start(self, force_build): ...
        async def stop(self, delete): ...
        async def upload_file(self, source_path, target_path): ...
        async def upload_dir(self, source_dir, target_dir): ...
        async def download_file(self, source_path, target_path): ...
        async def download_dir(self, source_dir, target_dir): ...
        async def exec(self, command, cwd=None, env=None, timeout_sec=None, user=None):
            return ExecResult(return_code=0)

    env = Plain(
        environment_dir=temp_dir,
        environment_name="plain",
        session_id="plain",
        trial_paths=TrialPaths(trial_dir=temp_dir),
        task_env_config=EnvironmentConfig(),
    )

    with pytest.raises(ServiceOperationsUnsupportedError):
        await env.service_upload_file("a", "/a", service="maya")
    with pytest.raises(ServiceOperationsUnsupportedError):
        await env.service_upload_dir("a", "/a", service="maya")
    await env.service_upload_file("a", "/a", service="main")
