"""Per-command cgroups for Sail execution cancellation."""

from uuid import uuid4

_DIRECT_SCOPE = r"""
set -eu
scope=$1; shift
chmod 700 "$scope"
trap 'touch "$scope/done"' EXIT
IFS=: read -r _ _ path < /proc/self/cgroup
printf '/sys/fs/cgroup%s\n' "$path" > "$scope/path"
mv "$scope/path" "$scope/ready"
exec "$@"
"""

_COMPOSE_SCOPE = r"""
set -eu
scope=$1; shift
chmod 700 "$scope"
trap 'if [ -n "${pid:-}" ] && [ ! -f "$scope/ready" ]; then kill -KILL "$pid"; fi; touch "$scope/done"' EXIT
"$@" &
client=$!
while :; do
  pid=$(ps -eo pid=,stat=,args= | awk -v scope="$scope" '$2 ~ /^T/ && index($0, scope) {print $1}')
  [ -z "$pid" ] || break
  if ! kill -0 "$client" 2>/dev/null; then wait "$client"; exit $?; fi
  sleep 0.01
done
IFS=: read -r _ _ path < "/proc/$pid/cgroup"
cgroup="/sys/fs/cgroup$path/${scope##*/}"
mkdir "$cgroup"
printf '%s\n' "$pid" > "$cgroup/cgroup.procs"
printf '%s\n' "$cgroup" > "$scope/path"
mv "$scope/path" "$scope/ready"
kill -CONT "$pid"
wait "$client"
"""

_CANCEL_SCOPE = r"""
set -eu
scope=$1
while [ ! -f "$scope/ready" ]; do
  [ -d "$scope" ] || exit 0
  [ ! -f "$scope/done" ] || exit 0
  sleep 0.01
done
read -r cgroup < "$scope/ready"
if [ -d "$cgroup" ]; then
  printf '1\n' > "$cgroup/cgroup.kill"
  while [ -d "$cgroup" ] && grep -q '^populated 1$' "$cgroup/cgroup.events"; do sleep 0.01; done
fi
"""


class SailExecScope:
    def __init__(self, argv: list[str], compose_command: int | None = None):
        self.path = f"/tmp/harbor-exec-{uuid4().hex}"
        script = _DIRECT_SCOPE
        if compose_command is not None:
            script = _COMPOSE_SCOPE
            argv = [
                *argv[:compose_command],
                "sh",
                "-c",
                'kill -STOP $$; exec "$@"',
                self.path,
                *argv[compose_command:],
            ]
        self.argv = ["bash", "-c", script, "harbor-exec", self.path, *argv]

    @property
    def cancel_argv(self) -> list[str]:
        return ["bash", "-c", _CANCEL_SCOPE, "harbor-exec-cancel", self.path]
