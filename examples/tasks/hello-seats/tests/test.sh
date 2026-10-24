#!/bin/bash

python3 - <<'PY'
import json
from pathlib import Path


def p1_posted() -> bool:
    try:
        lines = Path("/logs/world/notes.jsonl").read_text().splitlines()
        notes = [json.loads(line) for line in lines if line.strip()]
    except (OSError, ValueError):
        return False
    return any(
        note.get("author") == "p1" and note.get("text") == "hello from p1"
        for note in notes
    )


def left_a_record(seat: str) -> bool:
    agent_logs = Path(f"/logs/seats/{seat}/agent")
    return any(
        (agent_logs / name).is_file() and (agent_logs / name).stat().st_size > 0
        for name in ("trajectory.json", "oracle.txt")
    )


reward = int(p1_posted() and left_a_record("p1") and left_a_record("p2"))
Path("/logs/verifier/reward.txt").write_text(f"{reward}\n")
PY
