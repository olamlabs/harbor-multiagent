"""DinD compose providers upload into any compose service, as they download."""

from pathlib import Path

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.compose_service_ops import ComposeServiceOpsMixin
from harbor.environments.dind_compose import DinDComposeOps


class RecordingDinD(DinDComposeOps):
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def _compose_exec(
        self, subcommand: list[str], timeout_sec: int | None = None
    ) -> ExecResult:
        self.calls.append(("compose", subcommand))
        return ExecResult(return_code=0)

    async def _host_exec(
        self, command: str, timeout_sec: int | None = None
    ) -> ExecResult:
        self.calls.append(("host", command))
        return ExecResult(return_code=0)

    async def _stage_file_to_host(self, source_path: Path | str, host_path: str):
        self.calls.append(("stage", host_path))

    async def _stage_dir_to_host(self, source_dir: Path | str, host_dir: str):
        self.calls.append(("stage", host_dir))

    def copy(self) -> list[str]:
        return next(args for kind, args in self.calls if kind == "compose")

    def staged(self) -> str:
        return next(str(path) for kind, path in self.calls if kind == "stage")


async def test_service_upload_file_copies_into_the_service():
    dind = RecordingDinD()

    await dind.service_upload_file("/local/a.sh", "/opt/a.sh", service="p1")

    assert dind.copy() == ["cp", dind.staged(), "p1:/opt/a.sh"]


async def test_service_upload_dir_copies_contents_into_the_service():
    dind = RecordingDinD()

    await dind.service_upload_dir("/local/tools", "/opt/tools", service="p1")

    assert dind.copy() == ["cp", f"{dind.staged()}/.", "p1:/opt/tools"]


async def test_upload_without_a_service_still_targets_main():
    dind = RecordingDinD()

    await dind.upload_file("/local/a.sh", "/opt/a.sh")

    assert dind.copy()[-1] == "main:/opt/a.sh"


class MixinEnvironment(ComposeServiceOpsMixin):
    is_main_service = staticmethod(BaseEnvironment.is_main_service)

    def __init__(self, transport: RecordingDinD) -> None:
        self.transport = transport
        self.main_uploads: list[str] = []

    def _compose_service_transport(self, service: str | None) -> RecordingDinD:
        return self.transport

    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        self.main_uploads.append(target_path)


async def test_mixin_sends_main_uploads_to_the_environment_and_others_to_compose():
    transport = RecordingDinD()
    environment = MixinEnvironment(transport)

    await environment.service_upload_file("/local/a", "/opt/a", service="main")
    await environment.service_upload_file("/local/b", "/opt/b", service="p2")

    assert environment.main_uploads == ["/opt/a"]
    assert transport.copy()[-1] == "p2:/opt/b"
