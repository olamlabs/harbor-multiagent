"""Sail environment provider.

Each trial runs on a Sailbox, a microVM from Sail
(https://docs.sailresearch.com/sailboxes). Single-container tasks run
directly in the VM from the task's Dockerfile or ``docker_image``. A Docker
Compose task runs on a VM that has Docker: ``docker compose`` builds and
starts the project inside the VM, as the other cloud environments do. Network
policies map onto Sail's egress policy, which covers the whole VM in both
modes. A single-container VM has it from creation; a Compose VM has it once
its services are up, since building and pulling need the network.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import math
import logging
import os
import re
import shlex
import tempfile
from abc import abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Sequence
from contextlib import aclosing
from pathlib import Path
from typing import Any, cast, override

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from harbor.constants import MAIN_SERVICE_NAME
from harbor.environments.base import (
    BaseEnvironment,
    ExecResult,
    HealthcheckError,
    OutputStream,
)
from harbor.environments.capabilities import (
    EnvironmentCapabilities,
    EnvironmentResourceCapabilities,
)
from harbor.environments.compose_service_ops import (
    ComposeServiceOpsMixin,
    ComposeServiceTransport,
)
from harbor.environments.definition import (
    require_agent_environment_definition,
    should_use_prebuilt_docker_image,
)
from harbor.environments.dind_compose import DinDComposeOps
from harbor.environments.docker import (
    COMPOSE_BUILD_PATH,
    COMPOSE_PREBUILT_PATH,
    ENV_COMPOSE_NAME,
    RESOURCES_COMPOSE_NAME,
    self_bind_mount,
    write_mounts_compose_file,
    write_resources_compose_file,
)
from harbor.environments.docker.compose_env import (
    ComposeInfraEnvVars,
    legacy_log_mount_env_vars,
    merge_compose_env,
)
from harbor.environments.docker.docker import _sanitize_docker_image_name
from harbor.environments.options import EnvironmentOptions
from harbor.environments.ssh import SSHAccess, SSHAccessError, StreamHandle, run_local
from harbor.models.asp import ASPConfig, ASPConnection, ASPIdentityFile
from harbor.models.environment_type import EnvironmentType
from harbor.models.task.config import (
    EnvironmentConfig,
    HealthcheckConfig,
    NetworkMode,
    NetworkPolicy,
    TpuSpec,
)
from harbor.models.trial.config import ResourceMode, ServiceVolumeConfig
from harbor.models.trial.paths import TrialPaths
from harbor.utils.env import resolve_env_vars
from harbor.utils.optional_import import MissingExtraError

try:
    from sail import (
        App,
        Config,
        EgressPolicy,
        Image,
        ImageDefinition,
        RegistryLogin,
        Sailbox,
        Secret,
        TcpEndpoint,
    )
    from sail._core_client import MIN_DISK_GIB, memory_range_gib

    _HAS_SAIL = True
except ImportError:
    _HAS_SAIL = False

# The SDK caps each stream at 1 MiB by default and marks the result truncated;
# Harbor callers expect the whole output, so ask for the SDK's maximum.
_OUTPUT_BUFFER_BYTES = 64 * 1024 * 1024

# The variables that choose docker's daemon and configuration directory. Their
# host values describe this machine, so a task compose file that references
# one does not get them: docker on the Sailbox would try to reach this
# machine's daemon. Every other referenced host variable is forwarded.
_DOCKER_CONNECTION_VARS = frozenset(
    {
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CONFIG",
        "DOCKER_CERT_PATH",
        "DOCKER_TLS",
        "DOCKER_TLS_VERIFY",
    }
)


def _bash_argv(command: str, cwd: str | None = None) -> list[str]:
    """Return the argv that runs ``command`` under bash, as the Docker provider does.

    The SDK applies its ``cwd`` option only to string commands, which it runs
    under ``/bin/sh``, so the directory change is spelled out here. The
    command runs in its own ``bash -c`` after the ``cd &&``, so a failed
    ``cd`` runs none of it.
    """
    if cwd:
        command = f"cd {shlex.quote(cwd)} && exec bash -c {shlex.quote(command)}"
    return ["bash", "-c", command]


def _registry_logins(docker_config_path: str | Path | None) -> list[RegistryLogin]:
    """The logins in a Docker ``config.json``, for Sail's image builds.

    Each ``auths`` entry that carries a credential becomes a login for its
    registry host, with the password sent only with the request that uses it;
    an entry without one, as Docker writes when a credential helper holds the
    secret, is skipped. Docker Hub's entry is keyed by its index URL, which is
    the registry ``docker.io``.
    """
    if not docker_config_path:
        return []
    auths = json.loads(Path(docker_config_path).read_text()).get("auths", {})
    logins = []
    for registry, entry in auths.items():
        if entry.get("auth"):
            username, _, password = (
                base64.b64decode(entry["auth"]).decode().partition(":")
            )
        elif entry.get("username") or entry.get("password"):
            username, password = entry.get("username", ""), entry.get("password", "")
        else:
            continue
        host = registry.split("://", 1)[-1].split("/", 1)[0]
        logins.append(
            RegistryLogin(
                "docker.io" if host == "index.docker.io" else host,
                username=username,
                password=Secret.ephemeral(password),
            )
        )
    return logins


def _qualified_reference(image: str) -> str:
    """The image reference with its registry host, as Docker resolves it: a
    reference whose first component is not a host is on Docker Hub."""
    first, _, rest = image.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        return image
    return f"docker.io/{image}" if rest else f"docker.io/library/{image}"


def _referenced_env_names(text: str) -> set[str]:
    """Names read by ``$VAR`` and by every ``${VAR...}`` form.

    That covers defaults such as ``${VAR:-default}`` and ``${VAR-default}``,
    and a reference nested inside one. ``$$`` is an escaped dollar sign.
    """
    names = re.findall(r"\$\$|\$\{?([A-Za-z_][A-Za-z0-9_]*)", text)
    return set(names) - {""}


class _SailStrategy:
    """Execution strategy: direct single-VM or Docker Compose on the VM.

    The environment owns the Sailbox handle and the SDK calls; strategies
    decide where commands and files go.
    """

    def __init__(self, env: SailEnvironment):
        self._env = env

    @abstractmethod
    async def start(self, force_build: bool) -> None: ...

    @abstractmethod
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    async def attach(self) -> None: ...

    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        await self._env._sdk_upload_file(source_path, target_path)

    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        await self._env._sdk_upload_dir(source_dir, target_dir)

    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        await self._env._sdk_download_file(source_path, target_path)

    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        await self._env._sdk_download_dir(source_dir, target_dir)


class _SailDirect(_SailStrategy):
    """Single-container strategy: the agent and verifier run directly in the VM
    booted from the task's image."""

    @override
    async def start(self, force_build: bool) -> None:
        env = self._env

        docker_image = env.task_env_config.docker_image
        if docker_image and should_use_prebuilt_docker_image(
            env.environment_dir, docker_image=docker_image, force_build=force_build
        ):
            reference = _qualified_reference(docker_image)
            login = max(
                (
                    login
                    for login in env._registry_logins
                    if reference.startswith(f"{login.prefix}/")
                ),
                key=lambda login: len(login.prefix),
                default=None,
            )
            image = Image.from_registry(
                docker_image,
                username=login.username if login else None,
                password=login.password if login else None,
            )
        else:
            image = await Image.from_dockerfile.aio(
                env.environment_dir / "Dockerfile",
                context_dir=env.environment_dir,
                registry_logins=env._registry_logins or None,
            )
            if force_build:
                image = await image.build.aio(
                    force_build=True, timeout=env._build_timeout_sec
                )

        await env._create_sailbox(image)

        # The image need not contain the task's workdir. fs.mkdir runs as root,
        # so a workdir it creates is root-owned.
        if workdir := env.task_env_config.workdir:
            await env._require_sailbox().fs.mkdir.aio(workdir)

        result = await env.ensure_dirs(env._mount_targets(writable_only=True))
        if result is not None and result.return_code != 0:
            raise RuntimeError(f"Failed to create the log directories: {result.stderr}")

        await env._upload_environment_dir_after_start()

    @override
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        return await self._env._sdk_exec(
            _bash_argv(command, cwd), env=env, timeout_sec=timeout_sec, user=user
        )

    @override
    async def attach(self) -> None:
        self._env._require_sailbox().shell()


class _SailDinD(DinDComposeOps, _SailStrategy):
    """Docker Compose strategy for multi-container tasks.

    The Sailbox boots Sail's devbox image, which has Docker, and ``docker
    compose`` builds and pulls the project's images inside the VM. Any image
    reference Docker takes works, and private images are pulled with the
    Docker login in ``docker_config_path``.

    Topology:
        Local machine (harbor CLI)
          └── Sailbox (image holding the project's service images)
                ├── dockerd (started at boot)
                └── docker compose
                      ├── main        ← agent runs here, exec/upload/download target
                      ├── sidecar     ← additional services
                      └── ...
    """

    _COMPOSE_DIR = "/harbor/compose"
    _ENVIRONMENT_DIR = "/harbor/environment"
    _MOUNTS_COMPOSE_NAME = "docker-compose-mounts.json"
    # One project name for the image build and every compose command: compose
    # names a built service's image after the project.
    _PROJECT_NAME = "harbor"

    def __init__(self, env: SailEnvironment):
        super().__init__(env)
        self._use_prebuilt = False
        self._resolved_task_env = resolve_env_vars(env.task_env_config.env)

    # ── DinDComposeOps primitives ────────────────────────────────────────

    _SELF_BIND_LOG_DIRS = True
    # A command in main runs under a non-login bash, as it does on the docker
    # provider: the container already has the image's environment, and a login
    # shell's /etc/profile would replace the image's PATH.
    _MAIN_LOGIN_SHELL = False
    # A large artifact directory takes longer than the shared budgets to copy
    # out of a container; Terminal-Bench tasks hit that.
    _CP_FILE_TIMEOUT_SEC = 1800
    _CP_DIR_TIMEOUT_SEC = 1800

    @override
    async def _host_exec(
        self, command: str, timeout_sec: int | None = None
    ) -> ExecResult:
        return await self._env._sdk_exec(_bash_argv(command), timeout_sec=timeout_sec)

    @override
    async def _stage_file_to_host(self, source_path: Path | str, host_path: str):
        await self._env._sdk_upload_file(source_path, host_path)

    @override
    async def _stage_dir_to_host(self, source_dir: Path | str, host_dir: str):
        await self._env._sdk_upload_dir(source_dir, host_dir)

    @override
    async def _fetch_file_from_host(self, host_path: str, target_path: Path | str):
        await self._env._sdk_download_file(host_path, target_path)

    @override
    async def _fetch_dir_from_host(self, host_dir: str, target_dir: Path | str):
        await self._env._sdk_download_dir(host_dir, target_dir)

    def _compose_referenced_env_vars(self) -> dict[str, str]:
        """Host values of the variables the task's compose files reference,
        except the ones that would point docker at this machine's daemon."""
        content = "\n".join(path.read_text() for _, path in self._task_compose_files())
        return {
            name: os.environ[name]
            for name in _referenced_env_names(content) - _DOCKER_CONNECTION_VARS
            if name in os.environ
        }

    def _extra_compose_files(self) -> list[tuple[str, Path]]:
        """Each extra compose file with the path it is staged at on the VM."""
        return [
            (f"{self._COMPOSE_DIR}/docker-compose-extra-{index}.yaml", path)
            for index, path in enumerate(self._env.extra_docker_compose_paths)
        ]

    def _task_compose_files(self) -> list[tuple[str, Path]]:
        """The task's compose file, when it has one, then the extra ones, each
        with its path on the VM."""
        task_compose = self._env.environment_dir / "docker-compose.yaml"
        return [
            *(
                [(f"{self._ENVIRONMENT_DIR}/docker-compose.yaml", task_compose)]
                if task_compose.exists()
                else []
            ),
            *self._extra_compose_files(),
        ]

    def _infra_env_vars(self, context_dir: str) -> dict[str, str]:
        """Harbor infrastructure vars required by the compose templates."""
        env_vars = ComposeInfraEnvVars(
            main_image_name=_sanitize_docker_image_name(
                f"hb__{self._env.environment_name}"
            ),
            context_dir=context_dir,
            prebuilt_image_name=(
                self._env.task_env_config.docker_image if self._use_prebuilt else None
            ),
            cpus=self._env._effective_cpus,
            memory=f"{memory_mb}M"
            if (memory_mb := self._env._effective_memory_mb)
            else None,
        ).to_env_dict()
        env_vars.update(
            legacy_log_mount_env_vars(self._resolve_volumes(), host_value="target")
        )
        return env_vars

    def _user_env_vars(self) -> dict[str, str]:
        """Referenced host vars, overridden by task env, then by persistent env."""
        return {
            **self._compose_referenced_env_vars(),
            **self._resolved_task_env,
            **self._env._persistent_env,
        }

    def _compose_env_vars(self) -> dict[str, str]:
        """All environment variables for docker compose commands."""
        return merge_compose_env(
            user_env=self._user_env_vars(),
            infra_env=self._infra_env_vars(self._ENVIRONMENT_DIR),
            logger=self._env.logger,
            collision_label="Referenced/task/persistent env vars",
        )

    @property
    def _build_or_prebuilt_path(self) -> Path:
        return COMPOSE_PREBUILT_PATH if self._use_prebuilt else COMPOSE_BUILD_PATH

    def _compose_files(self) -> list[str]:
        """Every compose file on the VM, in the order compose reads them."""
        return [
            f"{self._COMPOSE_DIR}/{RESOURCES_COMPOSE_NAME}",
            f"{self._COMPOSE_DIR}/{self._build_or_prebuilt_path.name}",
            *(vm_path for vm_path, _ in self._task_compose_files()),
            f"{self._COMPOSE_DIR}/{ENV_COMPOSE_NAME}",
            # Compose merges volumes by target, so the mounts layer last: a task
            # volume on a Harbor-managed path such as /logs/agent must not replace
            # the self-bind mount that downloads read from.
            f"{self._COMPOSE_DIR}/{self._MOUNTS_COMPOSE_NAME}",
        ]

    def _resolve_volumes(self) -> list[ServiceVolumeConfig]:
        """Materialize Trial's mount intent for the VM filesystem (self-bind).

        See ``_DaytonaDinD._resolve_volumes`` for the full rationale.
        """
        return [
            self_bind_mount(m) if m.get("type") == "bind" else m
            for m in self._env._mounts
        ]

    async def _stage_mounts_compose_file(
        self, volumes: list[ServiceVolumeConfig]
    ) -> None:
        """Write the mounts compose override locally and upload it to the VM."""
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / self._MOUNTS_COMPOSE_NAME
            write_mounts_compose_file(local_path, volumes)
            await self._env._sdk_upload_file(
                local_path,
                f"{self._COMPOSE_DIR}/{self._MOUNTS_COMPOSE_NAME}",
            )

    async def _stage_resources_compose_file(self) -> None:
        """Write the resource policy compose override locally and upload it."""
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / RESOURCES_COMPOSE_NAME
            write_resources_compose_file(
                local_path,
                cpu_request=self._env._resource_request_value(
                    "cpu", auto_mode=ResourceMode.REQUEST
                ),
                cpu_limit=self._env._resource_limit_value(
                    "cpu", auto_mode=ResourceMode.REQUEST
                ),
                memory_limit_mb=self._env._resource_limit_value(
                    "memory", auto_mode=ResourceMode.REQUEST
                ),
            )
            await self._env._sdk_upload_file(
                local_path,
                f"{self._COMPOSE_DIR}/{RESOURCES_COMPOSE_NAME}",
            )

    def _compose_options(self) -> list[str]:
        """The options every compose command runs under."""
        return [
            "-p",
            self._PROJECT_NAME,
            "--project-directory",
            self._ENVIRONMENT_DIR,
            *(arg for file in self._compose_files() for arg in ("-f", file)),
        ]

    def _compose_argv(self, subcommand: list[str]) -> list[str]:
        # Call docker by absolute path, as the SDK does: the compose environment
        # may set a PATH that does not include it.
        return ["/usr/bin/docker", "compose", *self._compose_options(), *subcommand]

    @override
    async def _compose_exec(
        self,
        subcommand: list[str],
        timeout_sec: int | None = None,
        *,
        env: dict[str, str] | None = None,
    ) -> ExecResult:
        """Run a docker compose subcommand on the VM."""
        return await self._env._sdk_exec(
            self._compose_argv(subcommand),
            env={**self._compose_env_vars(), **(env or {})},
            timeout_sec=timeout_sec,
        )

    async def _ensure_main_workdir(self) -> None:
        """Create the task's workdir in the main service, as root.

        The image need not contain it, and compose does not create it: Harbor
        passes it to each exec, not as the service's ``working_dir``.
        """
        workdir = self._env.task_env_config.workdir
        if not workdir or workdir == "/":
            return
        result = await self.exec(
            f"mkdir -p {shlex.quote(workdir)}", cwd="/", user="root"
        )
        if result.return_code != 0:
            raise RuntimeError(
                f"Failed to create the working directory {workdir}: {result.stderr}"
            )

    async def _wait_until(
        self, what: str, check: Callable[[], Awaitable[ExecResult]]
    ) -> None:
        """Run ``check`` every second until it succeeds; on timeout, raise
        with the last attempt's output."""
        timeout_sec = self._env._build_timeout_sec
        result: ExecResult | None = None
        try:
            async with asyncio.timeout(timeout_sec):
                while (result := await check()).return_code != 0:
                    await asyncio.sleep(1)
        except TimeoutError:
            last = (
                f"{result.stdout or ''}{result.stderr or ''}".strip()
                if result
                else "no attempt finished"
            )
            raise TimeoutError(
                f"{what} not ready after {timeout_sec} seconds: {last}"
            ) from None

    async def _wait_for_docker(self) -> None:
        """Wait for the VM's Docker daemon, which starts at boot, to answer."""
        await self._wait_until(
            "The Sailbox's Docker daemon", lambda: self._host_exec("docker info")
        )

    async def _stage_docker_login(self) -> None:
        """Give the VM's Docker the logins in ``docker_config_path``, as the
        local docker environment uses the host's."""
        config = self._env._docker_config_path
        if not config:
            return
        home = (await self._host_exec('printf %s "$HOME"')).stdout or "/root"
        await self._host_exec(f"mkdir -p {shlex.quote(home)}/.docker")
        await self._env._sdk_upload_file(config, f"{home}/.docker/config.json")

    @override
    async def start(self, force_build: bool) -> None:
        env = self._env

        self._use_prebuilt = should_use_prebuilt_docker_image(
            env.environment_dir,
            docker_image=env.task_env_config.docker_image,
            force_build=force_build,
        )
        # Building and pulling need the network, so the VM starts open and
        # gets the task's policy once its services are up.
        await env._create_sailbox(
            Image.devbox("amd64"), egress_policy=EgressPolicy.allow_all()
        )
        await self._wait_for_docker()
        await self._stage_docker_login()

        await env._sdk_upload_file(
            self._build_or_prebuilt_path,
            f"{self._COMPOSE_DIR}/{self._build_or_prebuilt_path.name}",
        )
        await self._stage_resources_compose_file()
        await self._stage_env_compose_file(self._COMPOSE_DIR)
        await env._sdk_upload_dir(env.environment_dir, self._ENVIRONMENT_DIR)
        for vm_path, path in self._extra_compose_files():
            await env._sdk_upload_file(path, vm_path)

        volumes = self._resolve_volumes()
        await self._stage_mounts_compose_file(volumes)

        # Ensure each bind-mount source dir exists on the VM and is writable
        # by non-root agent/verifier users.
        bind_sources = [v["source"] for v in volumes if v.get("type") == "bind"]
        if bind_sources:
            quoted = shlex.join(bind_sources)
            await self._host_exec(f"mkdir -p {quoted} && chmod 777 {quoted}")

        env.logger.debug("Building compose services inside the Sailbox...")
        result = await self._compose_exec(["build"], timeout_sec=env._build_timeout_sec)
        if result.return_code != 0:
            raise RuntimeError(
                f"docker compose build failed: {result.stdout} {result.stderr}"
            )

        env.logger.debug("Starting compose services inside the Sailbox...")
        result = await self._compose_exec(
            ["up", "--detach", "--wait"], timeout_sec=env._build_timeout_sec
        )
        if result.return_code != 0:
            raise RuntimeError(
                f"docker compose up failed: {result.stdout} {result.stderr}"
            )

        policy = env._to_egress_policy(env.network_policy)
        if policy != EgressPolicy.allow_all():
            await env._require_sailbox().set_egress_policy.aio(policy)

        await self._ensure_main_workdir()
        await env._upload_environment_dir_after_start()

    @override
    async def attach(self) -> None:
        sailbox = self._env._require_sailbox()
        sailbox.shell(
            shlex.join(self._compose_argv(["exec", MAIN_SERVICE_NAME, "bash"])),
            env=self._compose_env_vars(),
        )


class SailOptions(EnvironmentOptions):
    """User-facing kwargs for SailEnvironment."""

    app_name: str = Field(
        default="harbor",
        description="Name of the Sail app that owns the Sailboxes.",
    )
    max_lifetime_seconds: int = Field(
        default=172800,
        description="Seconds after creation at which Sail terminates the Sailbox, even if Harbor never stops it.",
    )
    docker_config_path: str | None = Field(
        default=None,
        description="A Docker config.json whose registry logins pull private images.",
    )


class SailEnvironment(ComposeServiceOpsMixin, BaseEnvironment):
    """Runs trials on Sailboxes, Sail's microVM sandboxes."""

    options_model = SailOptions
    options: SailOptions

    @classmethod
    @override
    def preflight(cls, kwargs: dict[str, Any] | None = None) -> None:
        super().preflight(kwargs)
        if not _HAS_SAIL:
            raise MissingExtraError(package="sail", extra="sail")
        try:
            Config.from_env()
        except ValueError as exc:
            raise SystemExit(f"Sail requires authentication. {exc}") from exc

    @staticmethod
    @override
    def type() -> EnvironmentType:
        return EnvironmentType.SAIL

    @classmethod
    @override
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        # Sail rounds memory and disk limits up, so a task can get more than it
        # asks for: resources are honored as requests, not enforced as limits.
        return EnvironmentResourceCapabilities(cpu_request=True, memory_request=True)

    @property
    @override
    def capabilities(self) -> EnvironmentCapabilities:
        # An egress policy covers the whole VM, so compose services are
        # restricted the same way as a directly run task.
        return EnvironmentCapabilities(
            stream=True,
            disable_internet=True,
            network_allowlist=True,
            network_allowlist_hostnames=True,
            network_allowlist_wildcard_hostnames=True,
            network_allowlist_ipv4_addresses=True,
            network_allowlist_ipv6_addresses=False,
            network_allowlist_ipv4_cidrs=True,
            network_allowlist_ipv6_cidrs=False,
            dynamic_network_policy=True,
            docker_compose=True,
        )

    @property
    @override
    def _uses_compose(self) -> bool:
        return self._compose_mode

    @override
    def _validate_definition(self):
        require_agent_environment_definition(
            self.environment_dir,
            docker_image=self.task_env_config.docker_image,
            extra_docker_compose_paths=self.extra_docker_compose_paths,
        )

    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        *,
        logger: logging.Logger | None = None,
        override_cpus: int | float | None = None,
        override_memory_mb: int | None = None,
        override_storage_mb: int | None = None,
        override_gpus: int | None = None,
        override_tpu: TpuSpec | None = None,
        cpu_enforcement_policy: ResourceMode = ResourceMode.AUTO,
        memory_enforcement_policy: ResourceMode = ResourceMode.AUTO,
        persistent_env: dict[str, str] | None = None,
        mounts: list[ServiceVolumeConfig] | None = None,
        network_policy: NetworkPolicy | None = None,
        phase_network_policies: Sequence[NetworkPolicy] | None = None,
        extra_docker_compose: list[Path] | None = None,
        stream: bool = False,
        enable_environment_dir_upload: bool = True,
        auto_mounts: list[ServiceVolumeConfig] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize a SailEnvironment instance.

        Args:
            environment_dir: The directory containing the environment definition files.
            environment_name: The name identifier for this environment instance.
            session_id: Unique session identifier for this environment instance.
            trial_paths: Path configuration object containing trial-related directory
                paths.
            task_env_config: Environment configuration specifying resources (CPU,
                memory, storage) and the task image.
            app_name: Name of the Sail app that owns the Sailboxes. Sailboxes
                created with the same app name share one app, which is created
                on first use. Default is "harbor".
            max_lifetime_seconds: Seconds after creation at which Sail terminates
                the Sailbox, even if Harbor never stops it. Default is 172800
                (two days).
            docker_config_path: A Docker ``config.json`` whose registry logins
                pull private images, as the local docker environment uses the
                host's login: Sail's image build gets them for a
                single-container task, and the VM's Docker for a Compose
                task. Default is none.
            extra_docker_compose: Additional compose files layered onto the task's
                compose project. A non-empty list switches the environment to
                compose mode.
        """
        self.options = cast(SailOptions, self.parse_options(kwargs))
        options = self.options
        app_name = options.app_name
        max_lifetime_seconds = options.max_lifetime_seconds
        docker_config_path = (
            str(Path(options.docker_config_path).expanduser())
            if options.docker_config_path
            else None
        )
        if not _HAS_SAIL:
            raise MissingExtraError(package="sail", extra="sail")

        # Set before super().__init__, which reads ``_uses_compose``.
        self._compose_mode = (environment_dir / "docker-compose.yaml").exists() or bool(
            extra_docker_compose
        )

        super().__init__(
            environment_dir=environment_dir,
            environment_name=environment_name,
            session_id=session_id,
            trial_paths=trial_paths,
            task_env_config=task_env_config,
            logger=logger,
            override_cpus=override_cpus,
            override_memory_mb=override_memory_mb,
            override_storage_mb=override_storage_mb,
            override_gpus=override_gpus,
            override_tpu=override_tpu,
            cpu_enforcement_policy=cpu_enforcement_policy,
            memory_enforcement_policy=memory_enforcement_policy,
            persistent_env=persistent_env,
            mounts=mounts,
            network_policy=network_policy,
            phase_network_policies=phase_network_policies,
            extra_docker_compose=extra_docker_compose,
            stream=stream,
            enable_environment_dir_upload=enable_environment_dir_upload,
            auto_mounts=auto_mounts,
        )
        self._app_name = app_name
        self._max_lifetime_seconds = max_lifetime_seconds
        self._docker_config_path = docker_config_path
        self._registry_logins = _registry_logins(docker_config_path)
        self._sailbox: Sailbox | None = None
        self._strategy: _SailStrategy = (
            _SailDinD(self) if self._compose_mode else _SailDirect(self)
        )
        self.logger.debug(f"Selected strategy: {self._strategy.__class__.__name__}")

    @property
    def _build_timeout_sec(self) -> int:
        return round(self.task_env_config.build_timeout_sec)

    def _require_sailbox(self) -> Sailbox:
        if not self._sailbox:
            raise RuntimeError("Sailbox not found. Please start the environment first.")
        return self._sailbox

    @staticmethod
    def _to_egress_policy(network_policy: NetworkPolicy) -> dict[str, Any]:
        """Translate a Harbor network policy into a Sail egress policy document."""
        if network_policy.network_mode == NetworkMode.NO_NETWORK:
            return EgressPolicy.no_network()
        if network_policy.network_mode == NetworkMode.ALLOWLIST:
            hosts = network_policy.allowed_hosts
            return (
                EgressPolicy.allow_only(*hosts) if hosts else EgressPolicy.no_egress()
            )
        return EgressPolicy.allow_all()

    async def _create_sailbox(
        self, image: ImageDefinition, *, egress_policy: dict[str, Any] | None = None
    ) -> None:
        """Create the Sailbox and keep its handle in ``self._sailbox``.

        Without ``egress_policy`` the VM gets the task's network policy from
        the moment it is created.
        """
        cpus = self._effective_cpus
        memory_mb = self._effective_memory_mb
        storage_mb = self._effective_storage_mb
        vcpu_count = math.ceil(cpus) if cpus else None
        memory_range = memory_range_gib(vcpu_count or 1)
        if memory_range is None:
            raise ValueError(f"No Sailbox has {vcpu_count} vCPUs")
        app = await App.find.aio(self._app_name, mint_if_missing=True)
        self._sailbox = await Sailbox.create.aio(
            app=app,
            image=image,
            name=self.session_id,
            vcpu_count=vcpu_count,
            memory_limit_gib=max(memory_range[0], math.ceil(memory_mb / 1024))
            if memory_mb
            else None,
            disk_limit_gib=max(MIN_DISK_GIB, math.ceil(storage_mb / 1024))
            if storage_mb
            else None,
            max_lifetime_seconds=self._max_lifetime_seconds,
            image_build_timeout=self._build_timeout_sec,
            egress_policy=self._to_egress_policy(self.network_policy)
            if egress_policy is None
            else egress_policy,
        )

    async def _release_sailbox(self, delete: bool) -> None:
        if not self._sailbox:
            return
        sailbox = self._sailbox
        if delete:
            await sailbox.terminate.aio()
        else:
            await sailbox.sleep.aio()
            self.logger.debug(
                f"Put Sailbox {sailbox.sailbox_id} to sleep because "
                f"delete=False; reopen it with `sail box shell "
                f"{sailbox.sailbox_id}`."
            )
        self._sailbox = None

    async def _sdk_exec(
        self,
        argv: list[str],
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        sailbox = self._require_sailbox()
        callback = self._output_callback()
        if callback is None:
            result = await sailbox.run.aio(
                argv,
                env=env,
                timeout=timeout_sec,
                user=user,
                output_buffer_bytes=_OUTPUT_BUFFER_BYTES,
            )
        else:
            process = await sailbox.exec.aio(
                argv,
                env=env,
                timeout=timeout_sec,
                user=user,
                output_mode="pipe",
                output_buffer_bytes=_OUTPUT_BUFFER_BYTES,
            )

            async def read_output(
                chunks: AsyncGenerator[str, None], stream: OutputStream
            ) -> None:
                async with aclosing(chunks):
                    async for chunk in chunks:
                        await callback(chunk, stream)

            try:
                try:
                    async with asyncio.TaskGroup() as group:
                        group.create_task(read_output(process.stdout, "stdout"))
                        group.create_task(read_output(process.stderr, "stderr"))
                    result = await process.wait()
                except ExceptionGroup as exc:
                    if len(exc.exceptions) == 1:
                        raise exc.exceptions[0] from None
                    raise
            except BaseException:
                try:
                    await process.cancel(force=True)
                except Exception as exc:
                    self.logger.warning("Failed to cancel Sailbox command: %s", exc)
                raise
            finally:
                process.close()
        if result.timed_out:
            raise RuntimeError(f"Command timed out after {timeout_sec} seconds")
        if result.stdout_truncated or result.stderr_truncated:
            self.logger.warning(
                f"Command output exceeded {_OUTPUT_BUFFER_BYTES // 2**20} MiB on a "
                "stream; only the most recent output was kept."
            )
        return ExecResult(
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.exit_code,
        )

    async def _sdk_upload_file(self, source_path: Path | str, target_path: str) -> None:
        await self._require_sailbox().fs.upload_file.aio(source_path, target_path)

    async def _sdk_upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        await self._require_sailbox().fs.upload_dir.aio(source_dir, target_dir)

    async def _sdk_download_file(
        self, source_path: str, target_path: Path | str
    ) -> None:
        await self._require_sailbox().fs.download_file.aio(source_path, target_path)

    async def _sdk_download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        await self._require_sailbox().fs.download_dir.aio(source_dir, target_dir)

    @override
    async def start(self, force_build: bool) -> None:
        try:
            if self.stream_enabled and any(
                policy.network_mode == NetworkMode.NO_NETWORK
                for policy in [self.network_policy, *self._phase_network_policies]
            ):
                raise ValueError(
                    "Sail stream mode requires SSH ingress and cannot use a no_network policy."
                )
            await self._strategy.start(force_build)
            if self.stream_enabled:
                await self._require_sailbox().enable_ssh.aio()
        except BaseException:
            try:
                await self._release_sailbox(delete=True)
            except Exception as exc:
                self.logger.warning(
                    "Failed to terminate Sailbox after start failed: %s", exc
                )
            raise

    @property
    @override
    def stream_handle(self) -> StreamHandle:
        reference = {"sailbox_id": self._require_sailbox().sailbox_id}
        if isinstance(self._strategy, _SailDinD):
            reference.update(
                project=self._strategy._PROJECT_NAME, service=MAIN_SERVICE_NAME
            )
        return StreamHandle(
            provider=self.type().value,
            reference=reference,
            workspace=self.task_env_config.workdir,
        )

    @classmethod
    @override
    def connect_ssh(
        cls, handle: StreamHandle
    ) -> contextlib.AbstractAsyncContextManager[SSHAccess]:
        return _connect_ssh(handle)

    @override
    async def stop(self, delete: bool) -> None:
        await self._release_sailbox(delete)

    @override
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        return await self._strategy.exec(
            command,
            cwd=cwd or self.task_env_config.workdir,
            env=self._merge_env(env),
            timeout_sec=timeout_sec,
            user=self._resolve_user(user),
        )

    @override
    async def run_healthcheck(
        self, healthcheck: HealthcheckConfig | None = None
    ) -> None:
        try:
            await super().run_healthcheck(healthcheck)
        except HealthcheckError as exc:
            if self._compose_mode:
                raise
            # A Sailbox boots a single-container image without running its
            # ENTRYPOINT or CMD, where the Docker provider runs the ENTRYPOINT
            # with a sleep as its arguments. A check that waits for a service
            # started that way never passes here, and the generic failure
            # would not say why.
            raise HealthcheckError(
                f"{exc}\nA Sailbox runs neither the image's ENTRYPOINT nor its "
                "CMD; if the check waits for a service they would start, it "
                "cannot pass here, so start the service from a compose file "
                "instead."
            ) from exc

    @override
    async def _apply_network_policy(self, network_policy: NetworkPolicy) -> None:
        sailbox = self._require_sailbox()
        await sailbox.set_egress_policy.aio(self._to_egress_policy(network_policy))

    @override
    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        await self._strategy.upload_file(source_path, target_path)

    @override
    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        await self._strategy.upload_dir(source_dir, target_dir)

    @override
    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        await self._strategy.download_file(source_path, target_path)

    @override
    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        await self._strategy.download_dir(source_dir, target_dir)

    @override
    def _compose_service_transport(
        self, service: str | None
    ) -> ComposeServiceTransport:
        """Return the DinD strategy, or raise when not in compose mode."""
        if not isinstance(self._strategy, _SailDinD):
            raise self._compose_unsupported(service)
        return self._strategy

    @override
    async def attach(self) -> None:
        await self._strategy.attach()


class SailStreamReference(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    sailbox_id: str = Field(min_length=1, max_length=256, pattern=r"^[\w.-]+$")
    project: str | None = Field(
        default=None, min_length=1, max_length=256, pattern=r"^[a-z0-9][a-z0-9_-]*$"
    )
    service: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$",
    )

    @model_validator(mode="after")
    def require_compose_project_and_service(self) -> SailStreamReference:
        if (self.project is None) != (self.service is None):
            raise ValueError("Compose SSH access requires both project and service")
        return self


class _SailComposeSSHAccess(SSHAccess):
    def __init__(
        self, config: ASPConfig, reference: SailStreamReference, workspace: str | None
    ):
        super().__init__(config)
        self._host = SSHAccess(config.model_copy(update={"workspace": "/"}))
        self._reference = reference
        self._workspace = workspace
        self._container: str | None = None

    @override
    async def __aenter__(self) -> _SailComposeSSHAccess:
        await self._host.__aenter__()
        try:
            containers = (
                (
                    await self._host.run(
                        shlex.join(
                            [
                                "/usr/bin/docker",
                                "ps",
                                "-q",
                                "--filter",
                                f"label=com.docker.compose.project={self._reference.project}",
                                "--filter",
                                f"label=com.docker.compose.service={self._reference.service}",
                            ]
                        )
                    )
                )
                .decode()
                .split()
            )
            if len(containers) != 1:
                raise SSHAccessError(
                    "Compose SSH access requires exactly one running container for the service"
                )
            self._container = containers[0]
            workspace = self._workspace
            if workspace is None:
                workspace = (
                    json.loads(
                        (
                            await self._host.run(
                                shlex.join(
                                    [
                                        "/usr/bin/docker",
                                        "inspect",
                                        "--format",
                                        "{{json .Config.WorkingDir}}",
                                        self._container,
                                    ]
                                )
                            )
                        ).decode()
                    )
                    or "/"
                )
            self.config = self.config.model_copy(update={"workspace": workspace})
            return self
        except BaseException:
            await self._host.__aexit__(None, None, None)
            raise

    @override
    async def __aexit__(self, *exc: object) -> None:
        self._container = None
        await self._host.__aexit__(*exc)

    @contextlib.asynccontextmanager
    @override
    async def process(
        self, command: str, *, forward: tuple[int, int] | None = None
    ) -> AsyncIterator[asyncio.subprocess.Process]:
        if self._container is None:
            raise SSHAccessError("Compose SSH access must be opened before use")
        if forward is not None:
            raise SSHAccessError(
                "Sail Compose stream access runs commands in the service container; port forwarding is not supported"
            )
        wrapped = shlex.join(
            [
                "/usr/bin/docker",
                "exec",
                "-i",
                "-w",
                self.config.workspace,
                self._container,
                "sh",
                "-c",
                command,
            ]
        )
        async with self._host.process(wrapped) as process:
            yield process


@contextlib.asynccontextmanager
async def _connect_ssh(handle: StreamHandle) -> AsyncIterator[SSHAccess]:
    reference = SailStreamReference.model_validate(handle.reference)
    sailbox = await Sailbox.get.aio(reference.sailbox_id)
    if sailbox.status != "running":
        raise SSHAccessError("Sailbox is not running. Resume it before connecting.")
    listener = await sailbox.listener.aio(22, protocol="tcp")
    endpoint = listener.endpoint
    if not isinstance(endpoint, TcpEndpoint):
        raise SSHAccessError(
            "Sail SSH access is unavailable. Start the trial with --stream."
        )
    host_key = await sailbox.fs.read.aio("/etc/ssh/ssh_host_ed25519_key.pub")
    config = Config.from_env()
    with tempfile.TemporaryDirectory(prefix="harbor-sail-ssh-") as temporary:
        identity = Path(temporary) / "identity"
        await run_local(
            "ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(identity)
        )
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{config.sailbox_api_url.rstrip('/')}/v1/ssh/certificate",
                headers={"Authorization": f"Bearer {config.api_key}"},
                json={"public_key": identity.with_suffix(".pub").read_text()},
            )
            response.raise_for_status()
        identity.with_name("identity-cert.pub").write_text(
            response.json()["certificate"]
        )
        access_config = ASPConfig(
            version="0",
            transport="ssh",
            connection=ASPConnection(
                host=endpoint.host,
                port=endpoint.port,
                user="root",
                identity=ASPIdentityFile(file=str(identity)),
                host_key=" ".join(host_key.decode().split()[:2]),
            ),
            workspace=handle.workspace or "/",
        )
        access = (
            _SailComposeSSHAccess(access_config, reference, handle.workspace)
            if reference.service is not None
            else SSHAccess(access_config)
        )
        async with access:
            yield access
