"""Unit tests for SailEnvironment."""

import asyncio
import base64
import contextlib
import json
import logging
import shlex
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import httpx

pytest.importorskip("sail")

import harbor.environments.sail as sail_mod
from harbor.environments.base import (
    ExecResult,
    HealthcheckError,
    ServiceOperationsUnsupportedError,
)
from harbor.environments.sail import (
    SailEnvironment,
    _SailDinD,
    _SailDirect,
    _referenced_env_names,
)
from harbor.environments.ssh import SSHAccessError, StreamHandle
from harbor.models.task.config import (
    EnvironmentConfig,
    HealthcheckConfig,
    NetworkMode,
    NetworkPolicy,
)
from harbor.models.trial.config import ServiceVolumeConfig
from harbor.models.trial.paths import TrialPaths
from harbor.utils.optional_import import MissingExtraError

NO_NETWORK = NetworkPolicy(network_mode=NetworkMode.NO_NETWORK)
ALLOWLIST = NetworkPolicy(
    network_mode=NetworkMode.ALLOWLIST,
    allowed_hosts=["pypi.org", "*.github.com", "10.0.0.0/8"],
)


def _make_env(
    temp_dir: Path,
    *,
    compose: bool = False,
    cpus: int | None = 2,
    memory_mb: int | None = 4096,
    storage_mb: int | None = None,
    docker_image: str | None = None,
    workdir: str | None = None,
    task_env: dict[str, str] | None = None,
    persistent_env: dict[str, str] | None = None,
    mounts: list[ServiceVolumeConfig] | None = None,
    extra_docker_compose: list[Path] | None = None,
    network_policy: NetworkPolicy | None = None,
    phase_network_policies: list[NetworkPolicy] | None = None,
    environment_kwargs: dict[str, object] | None = None,
) -> SailEnvironment:
    env_dir = temp_dir / "environment"
    env_dir.mkdir(exist_ok=True)
    if compose:
        (env_dir / "docker-compose.yaml").write_text(
            "services:\n  main:\n    environment:\n      - CPUS=${CPUS:-2}\n"
        )
    elif docker_image is None:
        (env_dir / "Dockerfile").write_text("FROM ubuntu:22.04\n")

    trial_dir = temp_dir / "trial"
    trial_dir.mkdir(exist_ok=True)
    trial_paths = TrialPaths(trial_dir=trial_dir)
    trial_paths.mkdir()

    config: dict = {
        "cpus": cpus,
        "memory_mb": memory_mb,
        "storage_mb": storage_mb,
        "env": task_env or {},
    }
    if docker_image is not None:
        config["docker_image"] = docker_image
    if workdir is not None:
        config["workdir"] = workdir

    extra: dict = {}
    if persistent_env is not None:
        extra["persistent_env"] = persistent_env
    if mounts is not None:
        extra["mounts"] = mounts
    if extra_docker_compose is not None:
        extra["extra_docker_compose"] = extra_docker_compose
    if network_policy is not None:
        extra["network_policy"] = network_policy
    if phase_network_policies is not None:
        extra["phase_network_policies"] = phase_network_policies
    if environment_kwargs is not None:
        extra.update(environment_kwargs)

    return SailEnvironment(
        environment_dir=env_dir,
        environment_name="test-task",
        session_id="Test.Session.123",
        trial_paths=trial_paths,
        task_env_config=EnvironmentConfig(**config),
        **extra,
    )


def _dind(env: SailEnvironment) -> _SailDinD:
    strategy = env._strategy
    assert isinstance(strategy, _SailDinD)
    return strategy


def _capture_compose_exec(dind: _SailDinD) -> list[list[str]]:
    """Patch the strategy's compose runner and return the captured subcommands."""
    calls: list[list[str]] = []

    async def _fake_compose_exec(subcommand, timeout_sec=None):
        calls.append(list(subcommand))
        return ExecResult(return_code=0, stdout="", stderr="")

    dind._compose_exec = _fake_compose_exec  # type: ignore[method-assign]
    return calls


def _run_result(
    exit_code: int = 0,
    *,
    stdout: str = "",
    stderr: str = "",
    timed_out: bool = False,
    stdout_truncated: bool = False,
) -> SimpleNamespace:
    """The fields of a ``sail.Sailbox.run`` result."""
    return SimpleNamespace(
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        timed_out=timed_out,
        stdout_truncated=stdout_truncated,
        stderr_truncated=False,
    )


class _FileStream:
    """Stands in for the SDK's file stream: yields chunks, then raises ``error``."""

    def __init__(self, *chunks: bytes, error: Exception | None = None):
        self._chunks = chunks
        self._error = error
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk
        if self._error:
            raise self._error


def _compose_commands(sdk: SimpleNamespace) -> list[list[str]]:
    """Each ``docker compose`` command run on the VM, from its subcommand on."""
    commands = []
    for call in sdk.sailbox.run.aio.await_args_list:
        argv = call.args[0]
        if argv[:2] != ["/usr/bin/docker", "compose"]:
            continue
        start = min(
            argv.index(name) for name in ("build", "up", "exec") if name in argv
        )
        commands.append(argv[start:])
    return commands


def _fail_when(sdk: SimpleNamespace, matches, **fields) -> None:
    """Make the VM commands ``matches`` accepts fail, and the rest succeed."""

    async def run(argv, **kwargs):
        return _run_result(**fields) if matches(argv) else _run_result()

    sdk.sailbox.run.aio.side_effect = run


def _compose_up_failure(**fields) -> Exception:
    """The error ``sail.Sailbox.compose_up`` raises for a failed start."""
    return sail_mod.CommandFailedError("compose failed", result=_run_result(**fields))


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Mock the SDK's image, app, and Sailbox entry points."""
    sailbox = MagicMock()
    sailbox.sailbox_id = "sbx_123"
    sailbox.status = "running"
    sailbox.enable_ssh.aio = AsyncMock()
    sailbox.listener.aio = AsyncMock(
        return_value=SimpleNamespace(
            endpoint=sail_mod.TcpEndpoint(host="ssh.example.com", port=2222)
        )
    )
    sailbox.run.aio = AsyncMock(return_value=_run_result())
    sailbox.exec.aio = AsyncMock()
    sailbox.set_egress_policy.aio = AsyncMock()
    sailbox.terminate.aio = AsyncMock()
    sailbox.sleep.aio = AsyncMock()
    sailbox.fs.mkdir.aio = AsyncMock()
    sailbox.fs.read.aio = AsyncMock(return_value=b"ssh-ed25519 AAAA test-host\n")
    sailbox.fs.upload_file.aio = AsyncMock()
    sailbox.fs.download_file.aio = AsyncMock()
    sailbox.fs.upload_dir.aio = AsyncMock()
    sailbox.fs.download_dir.aio = AsyncMock()

    image = MagicMock()
    image.build.aio = AsyncMock()
    image_cls = MagicMock()
    image_cls.from_registry.return_value = image
    image_cls.from_dockerfile.aio = AsyncMock(return_value=image)
    app_cls = MagicMock()
    app_cls.find.aio = AsyncMock(return_value="app-handle")
    sailbox_cls = MagicMock()
    sailbox_cls.get.aio = AsyncMock(return_value=sailbox)
    sailbox_cls.create.aio = AsyncMock(return_value=sailbox)

    monkeypatch.setattr(sail_mod, "Image", image_cls)
    monkeypatch.setattr(sail_mod, "App", app_cls)
    monkeypatch.setattr(sail_mod, "Sailbox", sailbox_cls)
    return SimpleNamespace(
        Image=image_cls,
        App=app_cls,
        Sailbox=sailbox_cls,
        image=image,
        sailbox=sailbox,
    )


class TestPreflight:
    def test_missing_extra_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sail_mod, "_HAS_SAIL", False)
        with pytest.raises(MissingExtraError, match="harbor\\[sail\\]"):
            SailEnvironment.preflight()

    def test_missing_credentials_exit(self, monkeypatch: pytest.MonkeyPatch):
        config_cls = MagicMock()
        config_cls.from_env.side_effect = ValueError("Set SAIL_API_KEY.")
        monkeypatch.setattr(sail_mod, "Config", config_cls)
        with pytest.raises(SystemExit, match="authentication. Set SAIL_API_KEY."):
            SailEnvironment.preflight()

    def test_passes_with_credentials(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sail_mod, "Config", MagicMock())
        SailEnvironment.preflight()


class TestCapabilities:
    def test_supports_requests_not_limits(self):
        caps = SailEnvironment.resource_capabilities()
        assert caps is not None
        assert caps.cpu_request is True
        assert caps.memory_request is True
        assert caps.cpu_limit is False
        assert caps.memory_limit is False

    def test_network_policies_in_both_modes(self, temp_dir):
        caps = _make_env(temp_dir).capabilities
        compose_caps = _make_env(temp_dir, compose=True).capabilities
        assert compose_caps == caps
        assert caps.docker_compose is True
        assert caps.stream is True
        assert compose_caps.stream is True
        assert caps.network_allowlist is True
        assert caps.dynamic_network_policy is True


class TestSizeSelection:
    @pytest.mark.parametrize(
        ("cpus", "memory_mb", "storage_mb", "expected"),
        [
            (None, None, None, (None, None, None)),
            (1, 1500, 33 * 1024, (1, 16, 33)),
            (2, 2048, 4096, (2, 16, 8)),
            (4, 40 * 1024, None, (4, 40, None)),
            (32, 128 * 1024, 300 * 1024, (32, 128, 300)),
            (64, 256 * 1024, None, (64, 256, None)),
        ],
    )
    async def test_creates_a_sailbox_with_the_requested_vcpus_memory_and_disk(
        self, temp_dir, sdk, cpus, memory_mb, storage_mb, expected
    ):
        env = _make_env(temp_dir, cpus=cpus, memory_mb=memory_mb, storage_mb=storage_mb)

        await env.start(force_build=False)

        kwargs = sdk.Sailbox.create.aio.await_args.kwargs
        assert "size" not in kwargs
        assert (
            kwargs["vcpu_count"],
            kwargs["memory_limit_gib"],
            kwargs["disk_limit_gib"],
        ) == expected


class TestRegistryLogins:
    @staticmethod
    def _docker_config(temp_dir: Path) -> Path:
        path = temp_dir / "config.json"
        path.write_text(
            json.dumps(
                {
                    "auths": {
                        "ghcr.io": {
                            "auth": base64.b64encode(b"octocat:t0ken").decode()
                        },
                        "https://index.docker.io/v1/": {
                            "username": "hubuser",
                            "password": "hubpass",
                        },
                        "registry.example.com": {},
                    }
                }
            )
        )
        return path

    def _env(self, temp_dir: Path, **kwargs) -> SailEnvironment:
        return _make_env(
            temp_dir,
            environment_kwargs={
                "docker_config_path": str(self._docker_config(temp_dir))
            },
            **kwargs,
        )

    def test_reads_each_login_in_a_docker_config(self, temp_dir):
        logins = self._env(temp_dir)._registry_logins

        assert [(login.prefix, login.username) for login in logins] == [
            ("ghcr.io", "octocat"),
            ("docker.io", "hubuser"),
        ]
        assert all(login.password is not None for login in logins)

    async def test_an_image_build_gets_the_logins(self, temp_dir, sdk):
        env = self._env(temp_dir)

        await env.start(force_build=False)

        build = sdk.Image.from_dockerfile.aio.await_args.kwargs
        assert build["registry_logins"] == env._registry_logins

    async def test_a_compose_vm_gets_the_docker_config(self, temp_dir, sdk):
        env = self._env(temp_dir, compose=True)

        await env.start(force_build=False)

        uploaded = [
            call.args[1] for call in sdk.sailbox.fs.upload_file.aio.await_args_list
        ]
        assert "/root/.docker/config.json" in uploaded

    async def test_a_prebuilt_image_is_pulled_with_its_registry_login(
        self, temp_dir, sdk
    ):
        env = self._env(temp_dir, docker_image="ghcr.io/acme/app:1")

        await env.start(force_build=False)

        sdk.Image.from_registry.assert_called_once_with(
            "ghcr.io/acme/app:1",
            username="octocat",
            password=env._registry_logins[0].password,
        )

    async def test_a_docker_hub_shorthand_image_gets_the_hub_login(self, temp_dir, sdk):
        env = self._env(temp_dir, docker_image="acme/app:1")

        await env.start(force_build=False)

        sdk.Image.from_registry.assert_called_once_with(
            "acme/app:1",
            username="hubuser",
            password=env._registry_logins[1].password,
        )

    def test_a_home_relative_config_is_read_and_staged_from_home(
        self, temp_dir, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(temp_dir))
        monkeypatch.setenv("USERPROFILE", str(temp_dir))
        self._docker_config(temp_dir)

        env = _make_env(
            temp_dir, environment_kwargs={"docker_config_path": "~/config.json"}
        )

        assert env._docker_config_path == str(temp_dir / "config.json")
        assert [login.prefix for login in env._registry_logins] == [
            "ghcr.io",
            "docker.io",
        ]

    async def test_no_config_passes_no_logins(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)

        await env.start(force_build=False)

        uploaded = [
            call.args[1] for call in sdk.sailbox.fs.upload_file.aio.await_args_list
        ]
        assert "/root/.docker/config.json" not in uploaded


class TestStrategySelection:
    def test_dockerfile_selects_direct(self, temp_dir):
        assert isinstance(_make_env(temp_dir)._strategy, _SailDirect)

    def test_compose_file_selects_dind(self, temp_dir):
        assert isinstance(_make_env(temp_dir, compose=True)._strategy, _SailDinD)

    def test_extra_compose_selects_dind(self, temp_dir):
        extra = temp_dir / "extra.yaml"
        extra.write_text("services:\n  sidecar:\n    image: redis:7\n")
        env = _make_env(temp_dir, extra_docker_compose=[extra])
        assert isinstance(env._strategy, _SailDinD)


class TestNetworkPolicy:
    @pytest.mark.parametrize(
        ("network_policy", "expected"),
        [
            (NetworkPolicy(), {}),
            (NO_NETWORK, {"no_network": True}),
            (ALLOWLIST, {"allowlist": ["pypi.org", "*.github.com", "10.0.0.0/8"]}),
            (NetworkPolicy(network_mode=NetworkMode.ALLOWLIST), {"allowlist": []}),
        ],
    )
    def test_egress_policy_document(self, network_policy, expected):
        assert SailEnvironment._to_egress_policy(network_policy) == expected

    def test_ipv6_allowlist_rejected_at_construction(self, temp_dir):
        policy = NetworkPolicy(
            network_mode=NetworkMode.ALLOWLIST, allowed_hosts=["2001:db8::1"]
        )
        with pytest.raises(ValueError, match="IPv6"):
            _make_env(temp_dir, network_policy=policy)

    def test_phase_policies_are_validated(self, temp_dir):
        policy = NetworkPolicy(
            network_mode=NetworkMode.ALLOWLIST, allowed_hosts=["2001:db8::/32"]
        )
        with pytest.raises(ValueError, match="IPv6"):
            _make_env(temp_dir, phase_network_policies=[policy])

    async def test_start_creates_under_the_policy(self, temp_dir, sdk):
        env = _make_env(temp_dir, network_policy=NO_NETWORK)

        await env.start(force_build=False)

        assert sdk.Sailbox.create.aio.await_args.kwargs["egress_policy"] == {
            "no_network": True
        }
        sdk.sailbox.set_egress_policy.aio.assert_not_awaited()

    async def test_a_compose_vm_gets_the_policy_once_its_services_are_up(
        self, temp_dir, sdk
    ):
        env = _make_env(temp_dir, compose=True, network_policy=NO_NETWORK)

        await env.start(force_build=False)

        assert sdk.Sailbox.create.aio.await_args.kwargs["egress_policy"] == {}
        sdk.sailbox.set_egress_policy.aio.assert_awaited_once_with({"no_network": True})

    async def test_a_compose_vm_with_public_network_keeps_its_policy(
        self, temp_dir, sdk
    ):
        env = _make_env(temp_dir, compose=True)

        await env.start(force_build=False)

        sdk.sailbox.set_egress_policy.aio.assert_not_awaited()

    async def test_set_network_policy_switches_egress(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox

        await env.set_network_policy(NO_NETWORK)

        sdk.sailbox.set_egress_policy.aio.assert_awaited_once_with({"no_network": True})
        assert env.network_policy == NO_NETWORK


class TestDirectStart:
    async def test_builds_dockerfile_and_creates_sailbox(self, temp_dir, sdk):
        env = _make_env(temp_dir, cpus=4, memory_mb=40 * 1024)

        await env.start(force_build=False)

        sdk.Image.from_dockerfile.aio.assert_awaited_once_with(
            env.environment_dir / "Dockerfile",
            context_dir=env.environment_dir,
            registry_logins=None,
        )
        sdk.image.build.aio.assert_not_awaited()
        sdk.App.find.aio.assert_awaited_once_with("harbor", mint_if_missing=True)
        sdk.Sailbox.create.aio.assert_awaited_once_with(
            app="app-handle",
            image=sdk.image,
            name="Test.Session.123",
            vcpu_count=4,
            memory_limit_gib=40,
            disk_limit_gib=None,
            max_lifetime_seconds=172800,
            image_build_timeout=600,
            egress_policy={},
        )
        assert env._sailbox is sdk.sailbox
        sdk.sailbox.fs.mkdir.aio.assert_not_awaited()
        sdk.sailbox.run.aio.assert_not_awaited()

    async def test_force_build_creates_from_the_rebuilt_image(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        built = MagicMock()
        sdk.image.build.aio.return_value = built

        await env.start(force_build=True)

        sdk.image.build.aio.assert_awaited_once_with(force_build=True, timeout=600)
        assert sdk.Sailbox.create.aio.await_args.kwargs["image"] is built

    async def test_failed_log_directories_fail_start(self, temp_dir, sdk):
        mounts: list[ServiceVolumeConfig] = [
            {"type": "bind", "source": "/host/agent", "target": "/logs/agent"},
        ]
        env = _make_env(temp_dir, mounts=mounts)
        sdk.sailbox.run.aio.return_value = _run_result(1, stderr="chmod: failed")

        with pytest.raises(RuntimeError, match="log directories.*chmod: failed"):
            await env.start(force_build=False)

        sdk.sailbox.terminate.aio.assert_awaited_once()
        assert env._sailbox is None

    async def test_cancelled_create_reaches_sdk_cleanup(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        creating = asyncio.Event()
        cancelled = asyncio.Event()

        async def create(**kwargs):
            creating.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        sdk.Sailbox.create.aio = create
        task = asyncio.create_task(env.start(force_build=False))
        await creating.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
        assert env._sailbox is None

    async def test_failed_cleanup_preserves_start_error_and_handle(self, temp_dir, sdk):
        env = _make_env(temp_dir, workdir="/app")
        sdk.sailbox.fs.mkdir.aio.side_effect = ValueError("workdir failed")
        sdk.sailbox.terminate.aio.side_effect = RuntimeError("terminate failed")

        with pytest.raises(ValueError, match="workdir failed"):
            await env.start(force_build=False)

        assert env._sailbox is sdk.sailbox

    async def test_cancelled_start_terminates_created_sailbox(self, temp_dir, sdk):
        env = _make_env(temp_dir, workdir="/app")
        configuring = asyncio.Event()

        async def mkdir(*args):
            configuring.set()
            await asyncio.Event().wait()

        sdk.sailbox.fs.mkdir.aio.side_effect = mkdir
        task = asyncio.create_task(env.start(force_build=False))
        await configuring.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        sdk.sailbox.terminate.aio.assert_awaited_once()
        assert env._sailbox is None

    async def test_prebuilt_image_uses_registry_reference(self, temp_dir, sdk):
        env = _make_env(temp_dir, docker_image="ubuntu:22.04", workdir="/app")
        (env.environment_dir / "tests.sh").write_text("exit 0\n")

        await env.start(force_build=False)

        sdk.Image.from_registry.assert_called_once_with(
            "ubuntu:22.04", username=None, password=None
        )
        sdk.Image.from_dockerfile.aio.assert_not_awaited()
        assert sdk.Sailbox.create.aio.await_args.kwargs["image"] is sdk.image
        # The task's loose environment files travel with a prebuilt image.
        sdk.sailbox.fs.upload_dir.aio.assert_awaited_once_with(
            env.environment_dir, "/app"
        )

    async def test_creates_workdir_and_mount_targets(self, temp_dir, sdk):
        mounts: list[ServiceVolumeConfig] = [
            {"type": "bind", "source": "/host/agent", "target": "/logs/agent"},
        ]
        env = _make_env(temp_dir, workdir="/app", mounts=mounts)

        await env.start(force_build=False)

        sdk.sailbox.fs.mkdir.aio.assert_awaited_once_with("/app")
        assert "/logs/agent" in sdk.sailbox.run.aio.await_args.args[0][-1]

    async def test_app_name_and_max_lifetime_are_configurable(self, temp_dir, sdk):
        env = _make_env(
            temp_dir,
            environment_kwargs={"app_name": "evals", "max_lifetime_seconds": 3600},
        )

        await env.start(force_build=False)

        sdk.App.find.aio.assert_awaited_once_with("evals", mint_if_missing=True)
        assert sdk.Sailbox.create.aio.await_args.kwargs["max_lifetime_seconds"] == 3600


class TestSSHStream:
    @pytest.mark.parametrize("compose", [False, True])
    async def test_stream_start_enables_native_ssh_and_exports_handle(
        self, temp_dir, sdk, compose
    ):
        env = _make_env(
            temp_dir,
            compose=compose,
            workdir="/app",
            environment_kwargs={"stream": True},
        )

        await env.start(force_build=False)

        sdk.sailbox.enable_ssh.aio.assert_awaited_once_with()
        assert env.stream_handle == StreamHandle(
            provider="sail",
            reference={
                "sailbox_id": "sbx_123",
                **({"project": "harbor", "service": "main"} if compose else {}),
            },
            workspace="/app",
        )

    async def test_without_stream_does_not_enable_ssh(self, temp_dir, sdk):
        await _make_env(temp_dir).start(force_build=False)

        sdk.sailbox.enable_ssh.aio.assert_not_awaited()

    @pytest.mark.parametrize("phase_policy", [False, True])
    async def test_no_network_fails_before_creation(self, temp_dir, sdk, phase_policy):
        env = _make_env(
            temp_dir,
            network_policy=None if phase_policy else NO_NETWORK,
            phase_network_policies=[NO_NETWORK] if phase_policy else None,
            environment_kwargs={"stream": True},
        )

        with pytest.raises(ValueError, match="requires SSH ingress.*no_network"):
            await env.start(force_build=False)

        sdk.Sailbox.create.aio.assert_not_awaited()

    async def test_ssh_start_failure_terminates_sailbox(self, temp_dir, sdk):
        env = _make_env(temp_dir, environment_kwargs={"stream": True})
        sdk.sailbox.enable_ssh.aio.side_effect = RuntimeError("SSH failed")

        with pytest.raises(RuntimeError, match="SSH failed"):
            await env.start(force_build=False)

        sdk.sailbox.terminate.aio.assert_awaited_once()
        assert env._sailbox is None

    def test_handle_requires_started_sailbox(self, temp_dir):
        with pytest.raises(RuntimeError, match="start the environment first"):
            _make_env(temp_dir).stream_handle

    @pytest.fixture
    def ssh(self, monkeypatch):
        config = SimpleNamespace(
            sailbox_api_url="https://sail.example/", api_key="synthetic-key"
        )
        monkeypatch.setattr(sail_mod.Config, "from_env", lambda: config)

        async def keygen(*args):
            identity = Path(args[-1])
            identity.write_text("synthetic-private-key")
            identity.with_suffix(".pub").write_text("ssh-ed25519 synthetic-public-key")

        monkeypatch.setattr(sail_mod, "run_local", AsyncMock(side_effect=keygen))
        client = MagicMock()
        client.post = AsyncMock(
            return_value=httpx.Response(
                200,
                json={"certificate": "synthetic-certificate", "key_id": "test"},
                request=httpx.Request(
                    "POST", "https://sail.example/v1/ssh/certificate"
                ),
            )
        )
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(
            sail_mod.httpx, "AsyncClient", MagicMock(return_value=client)
        )
        access = MagicMock()
        access.__aenter__ = AsyncMock(return_value=access)
        access.__aexit__ = AsyncMock(return_value=False)
        access_cls = MagicMock(return_value=access)
        monkeypatch.setattr(sail_mod, "SSHAccess", access_cls)
        return SimpleNamespace(client=client, access=access, access_cls=access_cls)

    async def test_connect_uses_persisted_id_and_ephemeral_certificate(self, sdk, ssh):
        handle = StreamHandle(
            provider="sail", reference={"sailbox_id": "sbx_123"}, workspace="/app"
        )

        async with SailEnvironment.connect_ssh(handle) as access:
            assert access is ssh.access
            config = ssh.access_cls.call_args.args[0]
            assert config.connection.host == "ssh.example.com"
            assert config.connection.port == 2222
            assert config.connection.user == "root"
            assert config.connection.host_key == "ssh-ed25519 AAAA"
            assert config.workspace == "/app"
            identity = Path(config.connection.identity.file)
            assert identity.exists()
            assert (
                identity.with_name("identity-cert.pub").read_text()
                == "synthetic-certificate"
            )

        assert not identity.exists()
        ssh.access.__aexit__.assert_awaited_once()
        sdk.Sailbox.get.aio.assert_awaited_once_with("sbx_123")
        sdk.sailbox.listener.aio.assert_awaited_once_with(22, protocol="tcp")
        sdk.sailbox.fs.read.aio.assert_awaited_once_with(
            "/etc/ssh/ssh_host_ed25519_key.pub"
        )
        sail_mod.httpx.AsyncClient.assert_called_once_with()
        ssh.client.post.assert_awaited_once_with(
            "https://sail.example/v1/ssh/certificate",
            headers={"Authorization": "Bearer synthetic-key"},
            json={"public_key": "ssh-ed25519 synthetic-public-key"},
        )
        sdk.sailbox.enable_ssh.aio.assert_not_awaited()
        sdk.Sailbox.create.aio.assert_not_awaited()
        sdk.Sailbox.list.assert_not_called()
        sdk.sailbox.terminate.aio.assert_not_awaited()

    async def test_connect_never_resumes_stopped_box(self, sdk, ssh):
        sdk.sailbox.status = "paused"
        handle = StreamHandle(provider="sail", reference={"sailbox_id": "sbx_123"})

        with pytest.raises(SSHAccessError, match="not running"):
            async with SailEnvironment.connect_ssh(handle):
                pytest.fail("SSH connected to paused Sailbox")

        sdk.sailbox.resume.assert_not_called()
        sdk.sailbox.enable_ssh.aio.assert_not_awaited()
        ssh.client.post.assert_not_awaited()

    async def test_connect_requires_tcp_listener(self, sdk, ssh):
        sdk.sailbox.listener.aio.return_value = SimpleNamespace(endpoint=None)
        handle = StreamHandle(provider="sail", reference={"sailbox_id": "sbx_123"})

        with pytest.raises(SSHAccessError, match="Start the trial with --stream"):
            async with SailEnvironment.connect_ssh(handle):
                pytest.fail("SSH connected without TCP endpoint")

        ssh.client.post.assert_not_awaited()

    async def test_certificate_failure_propagates_without_connecting(self, sdk, ssh):
        ssh.client.post.return_value = httpx.Response(
            403,
            request=httpx.Request("POST", "https://sail.example/v1/ssh/certificate"),
        )
        handle = StreamHandle(provider="sail", reference={"sailbox_id": "sbx_123"})

        with pytest.raises(httpx.HTTPStatusError):
            async with SailEnvironment.connect_ssh(handle):
                pytest.fail("SSH connected without certificate")

        ssh.access_cls.assert_not_called()

    async def test_certificate_transport_error_is_not_retried(self, sdk, ssh):
        ssh.client.post.side_effect = httpx.ReadTimeout("Certificate request timed out")
        handle = StreamHandle(provider="sail", reference={"sailbox_id": "sbx_123"})

        with pytest.raises(httpx.ReadTimeout):
            async with SailEnvironment.connect_ssh(handle):
                pytest.fail("SSH connected without certificate")

        ssh.client.post.assert_awaited_once()
        ssh.access_cls.assert_not_called()
        identity = Path(sail_mod.run_local.await_args.args[-1])
        assert not identity.exists()

    @pytest.mark.parametrize("workspace", [None, "/task workdir"])
    async def test_compose_connect_runs_inside_service_with_its_default_user(
        self, sdk, ssh, workspace
    ):
        ssh.access.run = AsyncMock(side_effect=[b"abc123\n", b'"/home/player"\n'])
        commands = []
        process = MagicMock(returncode=0)
        process.communicate = AsyncMock(return_value=(b"output", b""))

        @contextlib.asynccontextmanager
        async def host_process(command, **kwargs):
            commands.append((command, kwargs))
            yield process

        ssh.access.process = host_process
        handle = StreamHandle(
            provider="sail",
            reference={
                "sailbox_id": "sbx_123",
                "project": "harbor",
                "service": "player",
            },
            workspace=workspace,
        )
        command = "printf '%s' \"a'b\"; cat"
        async with SailEnvironment.connect_ssh(handle) as access:
            assert isinstance(access, sail_mod._SailComposeSSHAccess)
            assert access.config.workspace == (workspace or "/home/player")
            assert await access.run(command, input=b"payload") == b"output"

        assert shlex.split(commands[0][0]) == [
            "/usr/bin/docker",
            "exec",
            "-i",
            "-w",
            workspace or "/home/player",
            "abc123",
            "sh",
            "-c",
            command,
        ]
        assert commands[0][1] == {}
        process.communicate.assert_awaited_once_with(b"payload")
        assert shlex.split(ssh.access.run.await_args_list[0].args[0]) == [
            "/usr/bin/docker",
            "ps",
            "-q",
            "--filter",
            "label=com.docker.compose.project=harbor",
            "--filter",
            "label=com.docker.compose.service=player",
        ]
        assert ssh.access.run.await_count == (2 if workspace is None else 1)
        assert ssh.access_cls.call_args.args[0].workspace == "/"
        ssh.access.__aexit__.assert_awaited_once()

    @pytest.mark.parametrize("containers", [b"", b"abc\ndef\n"])
    async def test_compose_requires_exactly_one_running_service_container(
        self, sdk, ssh, containers
    ):
        ssh.access.run = AsyncMock(return_value=containers)
        handle = StreamHandle(
            provider="sail",
            reference={
                "sailbox_id": "sbx_123",
                "project": "harbor",
                "service": "player",
            },
        )

        with pytest.raises(SSHAccessError, match="exactly one running container"):
            async with SailEnvironment.connect_ssh(handle):
                pytest.fail("Ambiguous service container was accepted")

        ssh.access.__aexit__.assert_awaited_once()

    async def test_compose_forwarding_is_explicitly_unsupported(self, sdk, ssh):
        ssh.access.run = AsyncMock(return_value=b"abc123\n")
        handle = StreamHandle(
            provider="sail",
            reference={"sailbox_id": "sbx_123", "project": "harbor", "service": "main"},
            workspace="/app",
        )

        async with SailEnvironment.connect_ssh(handle) as access:
            with pytest.raises(
                SSHAccessError,
                match="service container; port forwarding is not supported",
            ):
                async with access.process("serve", forward=(12345, 54321)):
                    pytest.fail("Compose port forwarding was accepted")

        ssh.access.process.assert_not_called()

    async def test_each_connection_pins_the_current_box_host_key(self, sdk, ssh):
        sdk.sailbox.fs.read.aio.side_effect = [
            b"ssh-ed25519 AAAA first\n",
            b"ssh-ed25519 BBBB second\n",
        ]
        for key in ["AAAA", "BBBB"]:
            async with SailEnvironment.connect_ssh(
                StreamHandle(provider="sail", reference={"sailbox_id": "sbx_123"})
            ):
                assert (
                    ssh.access_cls.call_args.args[0].connection.host_key
                    == f"ssh-ed25519 {key}"
                )

    async def test_missing_host_key_never_disables_verification(self, sdk, ssh):
        sdk.sailbox.fs.read.aio.side_effect = FileNotFoundError("host key missing")
        with pytest.raises(FileNotFoundError, match="host key missing"):
            async with SailEnvironment.connect_ssh(
                StreamHandle(provider="sail", reference={"sailbox_id": "sbx_123"})
            ):
                pytest.fail("SSH connected without a pinned host key")
        ssh.access_cls.assert_not_called()
        ssh.client.post.assert_not_awaited()

    @pytest.mark.parametrize(
        "reference",
        [
            {"project": "harbor"},
            {"service": "player"},
            {"project": "harbor", "service": "player; echo bad"},
        ],
    )
    async def test_compose_reference_is_validated_before_lookup(self, sdk, reference):
        handle = StreamHandle(
            provider="sail", reference={"sailbox_id": "sbx_123", **reference}
        )

        with pytest.raises(ValueError):
            async with SailEnvironment.connect_ssh(handle):
                pytest.fail("Invalid Compose reference was accepted")

        sdk.Sailbox.get.aio.assert_not_awaited()


class TestExec:
    async def test_runs_bash_in_workdir_with_merged_env(self, temp_dir, sdk):
        env = _make_env(temp_dir, workdir="/app", persistent_env={"KEEP": "1"})
        env._sailbox = sdk.sailbox
        sdk.sailbox.run.aio.return_value = _run_result(3, stdout="out", stderr="err")

        result = await env.exec(
            "echo hi", env={"FOO": "bar"}, timeout_sec=30, user="agent"
        )

        sdk.sailbox.run.aio.assert_awaited_once_with(
            ["bash", "-c", "cd /app && exec bash -c 'echo hi'"],
            env={"KEEP": "1", "FOO": "bar"},
            timeout=30,
            user="agent",
            output_buffer_bytes=64 * 1024 * 1024,
        )
        assert result == ExecResult(return_code=3, stdout="out", stderr="err")

    async def test_without_workdir_runs_in_the_image_workdir(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox

        await env.exec("id")

        assert sdk.sailbox.run.aio.await_args.args == (["bash", "-c", "id"],)

    async def test_default_user_is_passed_to_sdk(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        env.default_user = "agent"

        await env.exec("id")

        assert sdk.sailbox.run.aio.await_args.kwargs["user"] == "agent"

    async def test_timeout_raises(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        sdk.sailbox.run.aio.return_value = _run_result(124, timed_out=True)

        with pytest.raises(RuntimeError, match="timed out after 5 seconds"):
            await env.exec("sleep 10", timeout_sec=5)

    async def test_truncated_output_is_returned_with_a_warning(
        self, temp_dir, sdk, caplog
    ):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        sdk.sailbox.run.aio.return_value = _run_result(
            stdout="tail", stdout_truncated=True
        )

        with caplog.at_level(logging.WARNING):
            result = await env.exec("yes")

        assert result.stdout == "tail"
        assert "exceeded 64 MiB" in caplog.text

    async def test_before_start_raises(self, temp_dir):
        with pytest.raises(RuntimeError, match="start the environment first"):
            await _make_env(temp_dir).exec("true")


class TestStreamingExec:
    @pytest.mark.parametrize("compose", [False, True])
    async def test_streams_both_outputs_before_exit(self, temp_dir, sdk, compose):
        env = _make_env(
            temp_dir,
            compose=compose,
            workdir="/app",
            persistent_env={"KEEP": "1"},
            environment_kwargs={"stream": not compose},
        )
        env._sailbox = sdk.sailbox
        received = {"stdout": asyncio.Event(), "stderr": asyncio.Event()}
        finish = asyncio.Event()
        chunks = []

        async def output(stream):
            yield f"{stream} first"
            await finish.wait()
            yield " last\n"

        async def emit(text, stream):
            chunks.append((text, stream))
            received[stream].set()

        process = MagicMock()
        process.stdout = output("stdout")
        process.stderr = output("stderr")
        process.wait = AsyncMock(
            return_value=_run_result(
                3, stdout="stdout first last\n", stderr="stderr first last\n"
            )
        )
        process.cancel = AsyncMock()
        sdk.sailbox.exec.aio.return_value = process

        with env.scoped_output_callback(emit):
            task = asyncio.create_task(
                env.exec("command", env={"FOO": "bar"}, user="agent", timeout_sec=30)
            )
            try:
                async with asyncio.timeout(5):
                    await received["stdout"].wait()
                    await received["stderr"].wait()
                assert not task.done()
                process.wait.assert_not_awaited()
            finally:
                finish.set()
                result = await task

        assert result == ExecResult(
            return_code=3, stdout="stdout first last\n", stderr="stderr first last\n"
        )
        assert chunks[:2] == [("stdout first", "stdout"), ("stderr first", "stderr")]
        call = sdk.sailbox.exec.aio.await_args
        if compose:
            assert call.args[0][:2] == ["/usr/bin/docker", "compose"]
            assert call.args[0][-4:] == ["main", "bash", "-c", "command"]
            assert "-T" in call.args[0]
            assert "-w" in call.args[0]
            assert "-u" in call.args[0]
        else:
            assert call.args[0] == ["bash", "-c", "cd /app && exec bash -c command"]
            assert call.kwargs["user"] == "agent"
        assert call.kwargs["env"]["KEEP"] == "1"
        assert call.kwargs["env"]["FOO"] == "bar"
        assert call.kwargs["timeout"] == 30
        assert call.kwargs["output_mode"] == "pipe"
        assert call.kwargs["output_buffer_bytes"] == 64 * 1024 * 1024
        process.close.assert_called_once_with()
        process.cancel.assert_not_awaited()
        sdk.sailbox.run.aio.assert_not_awaited()

    async def test_without_callback_keeps_buffered_exec(self, temp_dir, sdk):
        env = _make_env(temp_dir, environment_kwargs={"stream": True})
        env._sailbox = sdk.sailbox

        await env.exec("true")

        sdk.sailbox.run.aio.assert_awaited_once()
        sdk.sailbox.exec.aio.assert_not_awaited()

    @pytest.mark.parametrize("truncated", [False, True])
    async def test_returns_sdk_result_without_replaying_it(
        self, temp_dir, sdk, caplog, truncated
    ):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        chunks = []

        async def output(text):
            yield text

        async def emit(text, stream):
            chunks.append((text, stream))

        process = MagicMock()
        process.stdout = output("before disconnect\n")
        process.stderr = output("")
        process.wait = AsyncMock(
            return_value=_run_result(
                stdout="buffered output\n", stdout_truncated=truncated
            )
        )
        sdk.sailbox.exec.aio.return_value = process

        with caplog.at_level(logging.WARNING), env.scoped_output_callback(emit):
            result = await env.exec("command")

        assert result.stdout == "buffered output\n"
        assert chunks == [("before disconnect\n", "stdout"), ("", "stderr")]
        assert ("exceeded 64 MiB" in caplog.text) == truncated

    @pytest.mark.parametrize(
        "failure", ["callback", "reader", "wait", "cancel", "timeout"]
    )
    async def test_failure_releases_process_and_preserves_error(
        self, temp_dir, sdk, failure
    ):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        reading = asyncio.Event()
        closed = {"stdout": False, "stderr": False}
        error = ValueError("stream failed")

        async def output(stream):
            try:
                reading.set()
                if failure == "cancel":
                    await asyncio.Event().wait()
                if failure == "reader" and stream == "stdout":
                    raise error
                yield stream
            finally:
                closed[stream] = True

        async def emit(text, stream):
            if failure == "callback":
                raise error

        process = MagicMock()
        process.stdout = output("stdout")
        process.stderr = output("stderr")
        process.wait = AsyncMock(
            return_value=_run_result(124, timed_out=failure == "timeout")
        )
        if failure == "wait":
            process.wait.side_effect = error
        process.cancel = AsyncMock()
        sdk.sailbox.exec.aio.return_value = process

        with env.scoped_output_callback(emit):
            task = asyncio.create_task(env.exec("command", timeout_sec=5))
            if failure == "cancel":
                await reading.wait()
                task.cancel()
            if failure == "timeout":
                with pytest.raises(RuntimeError, match="timed out after 5 seconds"):
                    await task
                process.cancel.assert_not_awaited()
            elif failure == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await task
                process.cancel.assert_awaited_once_with(force=True)
            elif failure == "callback":
                with pytest.raises(ExceptionGroup) as info:
                    await task
                assert all(exc is error for exc in info.value.exceptions)
                process.cancel.assert_awaited_once_with(force=True)
            else:
                with pytest.raises(ValueError) as info:
                    await task
                assert info.value is error
                process.cancel.assert_awaited_once_with(force=True)

        assert all(closed.values())
        process.close.assert_called_once_with()


class TestHealthcheck:
    CHECK = HealthcheckConfig(command="curl -sf http://localhost:8080/", retries=1)

    async def test_a_passing_check_raises_nothing(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox

        await env.run_healthcheck(self.CHECK)

        sdk.sailbox.run.aio.assert_awaited_once()

    async def test_direct_mode_failure_names_the_unrun_entrypoint(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        sdk.sailbox.run.aio.return_value = _run_result(7)

        with pytest.raises(HealthcheckError) as info:
            await env.run_healthcheck(self.CHECK)

        assert str(info.value) == (
            "Healthcheck failed after 1 consecutive retries: "
            "curl -sf http://localhost:8080/\n"
            "A Sailbox runs neither the image's ENTRYPOINT nor its CMD; if the "
            "check waits for a service they would start, it cannot pass here, so "
            "start the service from a compose file instead."
        )

    async def test_compose_mode_failure_is_the_generic_one(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)
        env._sailbox = sdk.sailbox
        sdk.sailbox.run.aio.return_value = _run_result(7)

        with pytest.raises(HealthcheckError) as info:
            await env.run_healthcheck(self.CHECK)

        assert str(info.value) == (
            "Healthcheck failed after 1 consecutive retries: "
            "curl -sf http://localhost:8080/"
        )


class TestFileTransfers:
    async def test_files_use_the_sdk_copies(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        source = temp_dir / "script.sh"
        source.write_text("#!/bin/sh\n")
        target = temp_dir / "out" / "result.txt"

        await env.upload_file(source, "/tmp/script.sh")
        await env.download_file("/tmp/result.txt", target)

        sdk.sailbox.fs.upload_file.aio.assert_awaited_once_with(
            source, "/tmp/script.sh"
        )
        sdk.sailbox.fs.download_file.aio.assert_awaited_once_with(
            "/tmp/result.txt", target
        )

    async def test_directories_use_sdk_transfers(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        source = temp_dir / "src"
        source.mkdir()

        await env.upload_dir(source, "/tmp/src")
        await env.download_dir("/tmp/out", temp_dir / "out")

        sdk.sailbox.fs.upload_dir.aio.assert_awaited_once_with(source, "/tmp/src")
        sdk.sailbox.fs.download_dir.aio.assert_awaited_once_with(
            "/tmp/out", temp_dir / "out"
        )


class TestStop:
    async def test_delete_terminates(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox

        await env.stop(delete=True)

        sdk.sailbox.terminate.aio.assert_awaited_once()
        sdk.sailbox.sleep.aio.assert_not_awaited()
        assert env._sailbox is None

    @pytest.mark.parametrize("compose", [False, True])
    async def test_keep_puts_sailbox_to_sleep(self, temp_dir, sdk, compose):
        env = _make_env(temp_dir, compose=compose)
        env._sailbox = sdk.sailbox

        await env.stop(delete=False)

        sdk.sailbox.sleep.aio.assert_awaited_once()
        sdk.sailbox.terminate.aio.assert_not_awaited()
        # Compose containers sleep with the VM; nothing is torn down first.
        sdk.sailbox.run.aio.assert_not_awaited()
        assert env._sailbox is None

    async def test_without_sailbox_is_noop(self, temp_dir):
        await _make_env(temp_dir).stop(delete=True)

    @pytest.mark.parametrize("delete", [False, True])
    async def test_release_failure_is_raised_and_keeps_handle(
        self, temp_dir, sdk, delete
    ):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        release = sdk.sailbox.terminate.aio if delete else sdk.sailbox.sleep.aio
        release.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await env.stop(delete=delete)

        assert env._sailbox is sdk.sailbox
        release.side_effect = None
        await env.stop(delete=delete)
        await env.stop(delete=delete)
        assert env._sailbox is None
        assert release.await_count == 2

    async def test_cancelled_stop_keeps_handle(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox
        sdk.sailbox.terminate.aio.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await env.stop(delete=True)

        assert env._sailbox is sdk.sailbox


class TestAttach:
    async def test_direct_opens_sdk_shell(self, temp_dir, sdk):
        env = _make_env(temp_dir)
        env._sailbox = sdk.sailbox

        await env.attach()

        sdk.sailbox.shell.assert_called_once_with()

    async def test_compose_opens_shell_in_main_service(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)
        env._sailbox = sdk.sailbox

        await env.attach()

        args, kwargs = sdk.sailbox.shell.call_args
        (command,) = args
        assert command.endswith(" exec main bash")
        assert command.startswith("/usr/bin/docker compose -p harbor ")
        assert kwargs["env"]["MAIN_IMAGE_NAME"] == "hb__test-task"


class TestDinDComposeCommand:
    def test_uses_harbor_project_and_compose_files(self, temp_dir):
        extra = temp_dir / "extra.yaml"
        extra.write_text("services:\n  sidecar:\n    image: redis:7\n")
        dind = _dind(_make_env(temp_dir, compose=True, extra_docker_compose=[extra]))

        argv = dind._compose_argv(["up", "-d"])

        files = dind._compose_files()
        assert argv == [
            "/usr/bin/docker",
            "compose",
            "-p",
            "harbor",
            "--project-directory",
            "/harbor/environment",
            *(arg for file in files for arg in ("-f", file)),
            "up",
            "-d",
        ]
        assert files == [
            f"/harbor/compose/{sail_mod.RESOURCES_COMPOSE_NAME}",
            f"/harbor/compose/{sail_mod.COMPOSE_BUILD_PATH.name}",
            "/harbor/environment/docker-compose.yaml",
            "/harbor/compose/docker-compose-extra-0.yaml",
            f"/harbor/compose/{sail_mod.ENV_COMPOSE_NAME}",
            # Last, so a task volume cannot replace a Harbor log mount.
            "/harbor/compose/docker-compose-mounts.json",
        ]


class TestDinDComposeEnvVars:
    def test_infra_vars_win_over_referenced_task_and_persistent_env(
        self, temp_dir, monkeypatch, caplog
    ):
        monkeypatch.setenv("CPUS", "999")
        env = _make_env(
            temp_dir,
            compose=True,
            task_env={"MEMORY": "1G", "CONTEXT_DIR": "/wrong"},
            persistent_env={"MAIN_IMAGE_NAME": "wrong-image"},
        )
        dind = _dind(env)

        with caplog.at_level(logging.WARNING):
            env_vars = dind._compose_env_vars()

        assert env_vars["CPUS"] == "2"
        assert env_vars["MEMORY"] == "4096M"
        assert env_vars["CONTEXT_DIR"] == "/harbor/environment"
        assert env_vars["MAIN_IMAGE_NAME"] == "hb__test-task"
        assert "CPUS" in caplog.text

    def test_unreferenced_host_vars_stay_on_the_host(self, temp_dir, monkeypatch):
        monkeypatch.setenv("UNREFERENCED", "secret")
        dind = _dind(_make_env(temp_dir, compose=True))

        assert "UNREFERENCED" not in dind._compose_env_vars()

    def test_docker_connection_vars_stay_on_the_host(self, temp_dir, monkeypatch):
        monkeypatch.setenv("DOCKER_HOST", "tcp://10.0.0.1:2375")
        monkeypatch.setenv("DOCKER_IMAGE", "redis")
        monkeypatch.setenv("COMPOSE_TAG", "7")
        env = _make_env(temp_dir, compose=True)
        (env.environment_dir / "docker-compose.yaml").write_text(
            "services:\n"
            "  main:\n"
            "    image: ${DOCKER_IMAGE}:${COMPOSE_TAG}\n"
            "    environment:\n"
            "      - DOCKER_HOST=${DOCKER_HOST:-unix:///var/run/docker.sock}\n"
        )

        env_vars = _dind(env)._compose_env_vars()

        assert env_vars["DOCKER_IMAGE"] == "redis"
        assert env_vars["COMPOSE_TAG"] == "7"
        assert "DOCKER_HOST" not in env_vars

    def test_task_env_wins_over_the_host(self, temp_dir, monkeypatch):
        monkeypatch.setenv("REDIS_TAG", "6")
        env = _make_env(temp_dir, compose=True, task_env={"REDIS_TAG": "7"})
        (env.environment_dir / "docker-compose.yaml").write_text(
            "services:\n  cache:\n    image: redis:${REDIS_TAG}\n"
        )

        assert _dind(env)._compose_env_vars()["REDIS_TAG"] == "7"


class TestReferencedEnvNames:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("$TAG", {"TAG"}),
            ("redis:${TAG}", {"TAG"}),
            ("redis:${TAG:-6}", {"TAG"}),
            ("redis:${TAG-6}", {"TAG"}),
            ("redis:${TAG:?required}", {"TAG"}),
            ("redis:${TAG:+alt}", {"TAG"}),
            ("${REGISTRY:-${MIRROR}}/redis", {"REGISTRY", "MIRROR"}),
            ("$REGISTRY/redis:$TAG", {"REGISTRY", "TAG"}),
            ("echo $$HOME costs $$5", set()),
        ],
    )
    def test_reads_every_reference_form(self, text, expected):
        assert _referenced_env_names(text) == expected


class TestDinDResourcesOverride:
    async def test_auto_policy_reserves_cpus_but_not_memory(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True, cpus=2, memory_mb=4096)
        env._sailbox = sdk.sailbox
        written: dict[str, dict] = {}

        async def capture(source, target):
            written[target] = json.loads(Path(source).read_bytes())

        sdk.sailbox.fs.upload_file.aio.side_effect = capture

        await _dind(env)._stage_resources_compose_file()

        main = written[f"/harbor/compose/{sail_mod.RESOURCES_COMPOSE_NAME}"][
            "services"
        ]["main"]
        assert main["deploy"]["resources"]["reservations"] == {"cpus": "2"}
        assert "cpus" not in main
        assert "mem_limit" not in main


class TestDinDReadiness:
    async def test_a_readiness_timeout_names_the_last_output(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)
        env.task_env_config.build_timeout_sec = 1
        env._sailbox = sdk.sailbox
        dind = _dind(env)
        dind._host_exec = AsyncMock(
            return_value=ExecResult(
                return_code=1, stdout="", stderr="Cannot connect to the daemon"
            )
        )

        with pytest.raises(TimeoutError, match="Cannot connect to the daemon"):
            await dind._wait_for_docker()


class TestDinDStart:
    async def test_builds_and_starts_the_project_in_the_vm(self, temp_dir, sdk):
        extra = temp_dir / "extra.yaml"
        extra.write_text("services:\n  sidecar:\n    image: redis:7\n")
        env = _make_env(temp_dir, compose=True, extra_docker_compose=[extra])

        await env.start(force_build=False)

        # The VM boots an image that has Docker, and builds, pulls and starts
        # the project itself.
        sdk.Image.devbox.assert_called_once_with("amd64")
        create = sdk.Sailbox.create.aio.await_args.kwargs
        assert create["image"] is sdk.Image.devbox.return_value
        assert _compose_commands(sdk) == [
            ["build"],
            ["up", "--detach", "--wait"],
        ]
        uploaded = {
            call.args[1] for call in sdk.sailbox.fs.upload_file.aio.await_args_list
        }
        assert uploaded == {
            f"/harbor/compose/{sail_mod.COMPOSE_BUILD_PATH.name}",
            f"/harbor/compose/{sail_mod.RESOURCES_COMPOSE_NAME}",
            f"/harbor/compose/{sail_mod.ENV_COMPOSE_NAME}",
            "/harbor/compose/docker-compose-extra-0.yaml",
            "/harbor/compose/docker-compose-mounts.json",
        }
        sdk.sailbox.fs.upload_dir.aio.assert_any_await(
            env.environment_dir, "/harbor/environment"
        )

    async def test_compose_wait_uses_the_build_timeout(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)
        env.task_env_config.build_timeout_sec = 73

        await env.start(force_build=False)

        up = next(
            call for call in sdk.sailbox.run.aio.await_args_list if "up" in call.args[0]
        )
        assert up.args[0][-3:] == ["up", "--detach", "--wait"]
        assert up.kwargs["timeout"] == 73

    async def test_unhealthy_service_preserves_compose_output(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)
        _fail_when(
            sdk,
            lambda argv: argv[-3:] == ["up", "--detach", "--wait"],
            exit_code=1,
            stdout="Container main Waiting",
            stderr="container main is unhealthy",
        )

        with pytest.raises(RuntimeError) as exc:
            await env.start(force_build=False)

        assert "docker compose up failed" in str(exc.value)
        assert "Container main Waiting" in str(exc.value)
        assert "container main is unhealthy" in str(exc.value)
        sdk.sailbox.terminate.aio.assert_awaited_once()

    async def test_extra_compose_without_a_task_compose_file(self, temp_dir, sdk):
        extra = temp_dir / "extra.yaml"
        extra.write_text("services:\n  sidecar:\n    image: redis:7\n")
        env = _make_env(temp_dir, extra_docker_compose=[extra])

        await env.start(force_build=False)

        paths = _dind(env)._compose_files()
        assert "/harbor/environment/docker-compose.yaml" not in paths
        assert "/harbor/compose/docker-compose-extra-0.yaml" in paths

    async def test_prebuilt_image_uses_the_prebuilt_file(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True, docker_image="python:3.12")

        await env.start(force_build=False)

        assert _dind(env)._compose_env_vars()["PREBUILT_IMAGE_NAME"] == "python:3.12"
        paths = _dind(env)._compose_files()
        assert f"/harbor/compose/{sail_mod.COMPOSE_PREBUILT_PATH.name}" in paths
        assert f"/harbor/compose/{sail_mod.COMPOSE_BUILD_PATH.name}" not in paths

    @pytest.mark.parametrize("step", ["build", "up"])
    async def test_build_or_up_failure_terminates_sailbox(self, temp_dir, sdk, step):
        env = _make_env(temp_dir, compose=True)
        _fail_when(sdk, lambda argv: step in argv, exit_code=1, stderr="no such image")

        with pytest.raises(RuntimeError, match=f"compose {step} failed.*no such image"):
            await env.start(force_build=False)

        sdk.sailbox.terminate.aio.assert_awaited_once()

    async def test_creates_the_workdir_in_the_main_service(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True, workdir="/app")

        await env.start(force_build=False)

        mkdir = sdk.sailbox.run.aio.await_args_list[-1].args[0]
        assert mkdir[:2] == ["/usr/bin/docker", "compose"]
        assert mkdir[-10:] == [
            "exec",
            "-T",
            "-w",
            "/",
            "-u",
            "root",
            "main",
            "bash",
            "-c",
            "mkdir -p /app",
        ]

    async def test_log_mounts_are_self_bound_and_read_from_the_vm(self, temp_dir, sdk):
        mounts: list[ServiceVolumeConfig] = [
            {"type": "bind", "source": "/host/agent", "target": "/logs/agent"},
        ]
        env = _make_env(temp_dir, compose=True, mounts=mounts)
        written: dict[str, bytes] = {}

        async def capture(source, target):
            written[target] = Path(source).read_bytes()

        sdk.sailbox.fs.upload_file.aio.side_effect = capture

        await env.start(force_build=False)
        await env.download_dir("/logs/agent", temp_dir / "agent")

        override = json.loads(written["/harbor/compose/docker-compose-mounts.json"])
        assert override["services"]["main"]["volumes"] == [
            {"type": "bind", "source": "/logs/agent", "target": "/logs/agent"}
        ]
        host_commands = [
            call.args[0][-1] for call in sdk.sailbox.run.aio.await_args_list
        ]
        assert "mkdir -p /logs/agent && chmod 777 /logs/agent" in host_commands
        sdk.sailbox.fs.download_dir.aio.assert_awaited_once_with(
            "/logs/agent", temp_dir / "agent"
        )

    async def test_workdir_failure_terminates_sailbox(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True, workdir="/app")
        _fail_when(
            sdk,
            lambda argv: argv[-1] == "mkdir -p /app",
            exit_code=1,
            stderr="read-only file system",
        )

        with pytest.raises(RuntimeError, match="/app.*read-only file system"):
            await env.start(force_build=False)

        sdk.sailbox.terminate.aio.assert_awaited_once()


class TestDinDExecRouting:
    async def test_exec_runs_in_the_main_service(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True, workdir="/app")
        env.default_user = "agent"
        env._sailbox = sdk.sailbox

        literal = "synthetic 'quote'\n$literal"
        await env.exec("id", env={"FOO": literal, "EMPTY": ""})

        call = sdk.sailbox.run.aio.await_args
        argv = call.args[0]
        assert argv[argv.index("exec") :] == [
            "exec",
            "-T",
            "-w",
            "/app",
            "-e",
            "FOO",
            "-e",
            "EMPTY",
            "-u",
            "agent",
            "main",
            "bash",
            "-c",
            "id",
        ]
        assert call.kwargs["env"]["FOO"] == literal
        assert call.kwargs["env"]["EMPTY"] == ""

    async def test_service_exec_targets_sidecar(self, temp_dir):
        env = _make_env(temp_dir, compose=True)
        calls = _capture_compose_exec(_dind(env))

        await env.service_exec("echo hi", service="sidecar")

        assert calls == [["exec", "-T", "sidecar", "sh", "-c", "echo hi"]]

    async def test_service_exec_without_compose_raises(self, temp_dir):
        with pytest.raises(ServiceOperationsUnsupportedError):
            await _make_env(temp_dir).service_exec("echo hi", service="sidecar")


class TestDinDTransfers:
    async def test_upload_copies_through_the_vm_and_removes_the_staging_file(
        self, temp_dir, sdk
    ):
        env = _make_env(temp_dir, compose=True)
        env._sailbox = sdk.sailbox
        source = temp_dir / "script.sh"
        source.write_text("#!/bin/sh\n")

        await env.upload_file(source, "/app/script.sh")

        (_source, staged), _ = sdk.sailbox.fs.upload_file.aio.await_args
        copy, cleanup = sdk.sailbox.run.aio.await_args_list
        assert copy.args[0][-3:] == ["cp", staged, "main:/app/script.sh"]
        assert copy.kwargs["timeout"] == 1800
        assert cleanup.args[0][-1] == f"rm -f {staged}"

    async def test_failed_cleanup_does_not_fail_the_transfer(
        self, temp_dir, sdk, caplog
    ):
        env = _make_env(temp_dir, compose=True)
        env._sailbox = sdk.sailbox
        source = temp_dir / "script.sh"
        source.write_text("#!/bin/sh\n")
        sdk.sailbox.run.aio.side_effect = [
            _run_result(),
            _run_result(124, timed_out=True),
        ]

        with caplog.at_level(logging.WARNING):
            await env.upload_file(source, "/app/script.sh")

        assert "Failed to remove /tmp/harbor_" in caplog.text

    async def test_failed_transfer_keeps_its_error_through_cleanup(self, temp_dir, sdk):
        env = _make_env(temp_dir, compose=True)
        env._sailbox = sdk.sailbox
        sdk.sailbox.run.aio.side_effect = [
            _run_result(1, stderr="no such file"),
            _run_result(124, timed_out=True),
        ]

        with pytest.raises(RuntimeError, match="compose cp failed.*no such file"):
            await env.download_file("/app/result.txt", temp_dir / "result.txt")
