"""Sync native session logs over SSH and reuse the agent's ATIF converter."""

import asyncio
import contextlib
import json
import shlex
import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path, PurePosixPath

from harbor.agents.base import BaseAgent
from harbor.agents.installed.base import BaseInstalledAgent
from harbor.environments.base import BaseEnvironment
from harbor.environments.factory import EnvironmentFactory
from harbor.environments.ssh import SSHAccess
from harbor.models.trajectories import ContentPart, Trajectory
from harbor.utils.path_safety import validate_output_path

POLL_INTERVAL_SEC = 2


def _write_trajectory(
    agent: BaseInstalledAgent, directory: Path, logs_dir: Path
) -> bool:
    """Convert the mirrored session logs and publish the ATIF in ``directory``.

    Returns whether a trajectory was published.
    """
    trajectory = agent.convert_trajectory(logs_dir)
    if trajectory is None:
        return False
    directory.mkdir(parents=True, exist_ok=True)
    try:
        _copy_referenced_media(agent.logs_dir, trajectory, directory)
    except OSError:
        agent.logger.debug("Streamed trajectory media not ready", exc_info=True)
        return False
    temporary_path = directory / "trajectory.tmp"
    temporary_path.write_text(
        json.dumps(trajectory.to_json_dict(), ensure_ascii=False),
        encoding="utf-8",
    )
    temporary_path.replace(directory / "trajectory.json")
    return True


def _referenced_media_paths(trajectory: Trajectory) -> set[str]:
    paths: set[str] = set()
    for step in trajectory.steps:
        contents: list[str | list[ContentPart] | None] = [step.message]
        if step.observation is not None:
            contents.extend(result.content for result in step.observation.results)
        for content in contents:
            if not isinstance(content, list):
                continue
            for part in content:
                if part.source is not None:
                    paths.add(part.source.path)
    for subagent in trajectory.subagent_trajectories or []:
        paths.update(_referenced_media_paths(subagent))
    return paths


def _copy_referenced_media(
    source_dir: Path, trajectory: Trajectory, directory: Path
) -> None:
    """Copy the media files a streamed trajectory references beside it.

    A converter saves images and audio next to the agent's own logs dir,
    which is where the streamed ATIF lands too, except in multi-step trials:
    their live trajectory.json goes under the step's agent dir, so relative
    media paths would dangle until the step's logs are archived there.

    A reference that is unsafe to follow is skipped; a copy that fails
    raises ``OSError`` so the caller can hold the trajectory back.
    """
    if source_dir == directory:
        return
    for reference in _referenced_media_paths(trajectory):
        path = Path(reference)
        if "://" in reference or path.anchor or ".." in path.parts or not path.parts:
            continue
        source, target = source_dir / path, directory / path
        try:
            validate_output_path(source, source_dir)
            validate_output_path(target, directory)
            if target.exists() or not source.is_file():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = target.with_name(f"{target.name}.tmp")
            validate_output_path(temporary_path, directory)
            shutil.copyfile(source, temporary_path)
            temporary_path.replace(target)
        except ValueError:
            continue


async def _mirror_session_logs(
    access: SSHAccess, remote_dir: PurePosixPath, mirror: Path
) -> bool:
    """Extend the local copy of the remote ``*.jsonl`` session logs.

    Native session logs only grow, so each file is read from the end of its
    local copy, and only whole lines are kept: the copy never holds a line the
    agent is still writing. A file that shrank is copied again and a file
    that disappeared is removed. Only transcripts are read, never
    configuration or credentials. Returns whether the copy changed.
    """
    cd = f"cd {shlex.quote(str(remote_dir))} && "
    listing = await access.run(cd + "find . -type f -name '*.jsonl' -exec wc -c {} +")
    remote: dict[str, int] = {}
    for line in listing.decode().splitlines():
        size, _, name = line.strip().partition(" ")
        path = PurePosixPath(name)
        if name.startswith("./") and ".." not in path.parts:
            remote[path.as_posix()] = int(size)

    changed = False
    for path in list(mirror.rglob("*.jsonl")):
        if path.relative_to(mirror).as_posix() not in remote:
            path.unlink()
            changed = True
    for name, size in remote.items():
        target = mirror / name
        validate_output_path(target, mirror)
        offset = target.stat().st_size if target.exists() else 0
        if size == offset:
            continue
        if size < offset:
            offset = 0
        data = await access.run(
            cd + f"tail -c +{offset + 1} -- {shlex.quote(name)}", timeout=None
        )
        complete = data[: data.rfind(b"\n") + 1]
        if offset and not complete:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("ab" if offset else "wb") as file:
            file.write(complete)
        changed = True
    return changed


@contextlib.asynccontextmanager
async def sync_trajectory(
    environment: BaseEnvironment,
    agent: BaseAgent,
    directory: Path,
    *,
    enabled: bool = True,
) -> AsyncIterator[None]:
    if (
        not enabled
        or not environment.stream_enabled
        or not isinstance(agent, BaseInstalledAgent)
        or agent.remote_session_logs_dir is None
    ):
        yield
        return
    stopping = asyncio.Event()
    remote_dir = agent.remote_session_logs_dir

    async def poll(logs_dir: Path) -> None:
        unpublished = False
        while not stopping.is_set():
            try:
                async with EnvironmentFactory.connect_ssh(
                    environment.stream_handle
                ) as access:
                    while True:
                        try:
                            if await _mirror_session_logs(
                                access, remote_dir, logs_dir / "sessions"
                            ):
                                unpublished = True
                            if unpublished:
                                unpublished = not await asyncio.to_thread(
                                    _write_trajectory, agent, directory, logs_dir
                                )
                        except ValueError:
                            environment.logger.debug(
                                "Trajectory not ready", exc_info=True
                            )
                        if stopping.is_set():
                            return
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(stopping.wait(), POLL_INTERVAL_SEC)
            except Exception:
                environment.logger.debug("Trajectory download failed", exc_info=True)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stopping.wait(), POLL_INTERVAL_SEC)

    # Finish polling before post-run conversion writes the authoritative ATIF.
    # Keeping the agent outside this task preserves its original exception type.
    with tempfile.TemporaryDirectory(prefix="harbor-stream-") as logs_dir:
        worker = asyncio.create_task(poll(Path(logs_dir)))
        try:
            yield
        finally:
            stopping.set()
            with contextlib.suppress(Exception):
                await worker
