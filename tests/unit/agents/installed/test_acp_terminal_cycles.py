"""ACP invocation lifecycle boundaries in persisted Harbor trajectories."""

import json

import pytest
from jsonschema import Draft202012Validator

from harbor.agents.installed.acp import AcpAgent
from harbor.models.agent.context import AgentContext
from harbor.models.trajectories import Trajectory


def _event(session_update, **fields):
    return {
        "event_type": "session_update",
        "payload": {"update": {"sessionUpdate": session_update, **fields}},
    }


def _call(call_id, **fields):
    return _event(
        "tool_call",
        toolCallId=call_id,
        title="shell",
        status="pending",
        rawInput={"command": f"run {call_id}"},
        **fields,
    )


def _update(call_id, **fields):
    return _event("tool_call_update", toolCallId=call_id, **fields)


def _text(text, session_update="agent_message_chunk"):
    return _event(session_update, content={"type": "text", "text": text})


def _call_ids(step):
    return [call.tool_call_id for call in step.tool_calls or []]


@pytest.fixture
def persist_trajectory(tmp_path):
    """Exercise loading, conversion and writing, then validate the saved schema."""
    agent = AcpAgent(
        logs_dir=tmp_path,
        registry_entry={
            "id": "test-acp",
            "name": "Test ACP",
            "version": "1.0",
            "description": "Synthetic event replay",
            "distribution": {"local": {"cmd": "unused"}},
        },
    )
    summary = {
        "instruction": "Run the requested tools",
        "session": {"sessionId": "terminal-cycles"},
        "resolved_session_model_id": "recorded-model",
        "prompt_response": {
            "stopReason": "end_turn",
            "usage": {
                "inputTokens": 101,
                "outputTokens": 12,
                "cachedReadTokens": 7,
                "totalTokens": 113,
            },
        },
        "latest_usage_update": {"cost": {"amount": 0.12, "currency": "USD"}},
    }
    summary_path = tmp_path / "acp-summary.json"
    summary_path.write_text(json.dumps(summary))
    original_summary = summary_path.read_bytes()
    validator = Draft202012Validator(Trajectory.model_json_schema())

    def persist(events):
        events_path = tmp_path / "acp-events.jsonl"
        events_path.write_text(
            "".join(json.dumps(event) + "\n" for event in events)
        )
        original_events = events_path.read_bytes()
        context = AgentContext()
        agent.populate_context_post_run(context)

        # Conversion errors are caught in production, so require the actual file.
        trajectory_path = tmp_path / "trajectory.json"
        assert trajectory_path.is_file()
        saved = json.loads(trajectory_path.read_text())
        validator.validate(saved)
        trajectory = Trajectory.model_validate(saved)
        assert trajectory.to_json_dict() == saved
        assert summary_path.read_bytes() == original_summary
        assert events_path.read_bytes() == original_events
        assert trajectory.schema_version == "ATIF-v1.6"
        assert trajectory.session_id == "terminal-cycles"
        assert trajectory.agent.model_name == "recorded-model"
        assert trajectory.steps[0].message == "Run the requested tools"
        assert [step.step_id for step in trajectory.steps] == list(
            range(1, len(trajectory.steps) + 1)
        )
        assert trajectory.final_metrics is not None
        assert trajectory.final_metrics.total_steps == len(trajectory.steps)
        assert trajectory.final_metrics.total_prompt_tokens == 101
        assert trajectory.final_metrics.total_completion_tokens == 12
        assert trajectory.final_metrics.total_cost_usd == 0.12
        assert (
            sum(
                step.metrics.prompt_tokens or 0
                for step in trajectory.steps
                if step.metrics is not None
            )
            == 101
        )
        assert (
            sum(
                step.metrics.completion_tokens or 0
                for step in trajectory.steps
                if step.metrics is not None
            )
            == 12
        )
        assert context.n_input_tokens == 101
        assert context.n_output_tokens == 12
        assert context.n_cache_tokens == 7
        assert context.cost_usd == 0.12
        return trajectory

    return persist


@pytest.mark.parametrize("status", ["failed", "completed"])
@pytest.mark.parametrize("following", ["message", "thought", "call"])
@pytest.mark.parametrize("repeats", [1, 2])
def test_terminal_cycle_separates_next_response(
    persist_trajectory, status, following, repeats
):
    output = "Error: invocation detail\nraw Unicode: café 🙂\n"
    terminal = _update("first", status=status, rawOutput=output)
    events = [_text("Trying"), _call("first"), *([terminal] * repeats)]
    if following == "call":
        events.append(_call("second"))
    else:
        kind = (
            "agent_thought_chunk" if following == "thought" else "agent_message_chunk"
        )
        events.append(_text("Next response", kind))

    trajectory = persist_trajectory(events)

    assert len(trajectory.steps) == 3
    first, second = trajectory.steps[1:]
    assert first.message == "Trying"
    assert _call_ids(first) == ["first"]
    assert first.tool_calls[0].arguments == {"command": "run first"}
    assert first.observation is not None
    assert [result.source_call_id for result in first.observation.results] == ["first"]
    # Repeated output remains recorded verbatim, without deduplication.
    assert first.observation.results[0].content == "\n\n".join([output] * repeats)
    assert first.extra["session_update_counts"]["tool_call_update"] == repeats
    if following == "call":
        assert _call_ids(second) == ["second"]
    else:
        assert second.tool_calls is None
        if following == "thought":
            assert second.reasoning_content == "Next response"
        else:
            assert second.message == "Next response"


@pytest.mark.parametrize("status", ["failed", "completed"])
@pytest.mark.parametrize("include_start", [True, False])
def test_terminal_cycle_at_end_of_events(persist_trajectory, status, include_start):
    events = [_call("last")] if include_start else []
    events.append(
        _update(
            "last",
            status=status,
            title="shell",
            content=[{"type": "content", "content": {"type": "text", "text": "final"}}],
        )
    )

    trajectory = persist_trajectory(events)

    assert len(trajectory.steps) == 2
    last = trajectory.steps[1]
    assert _call_ids(last) == ["last"]
    assert last.observation.results[0].content == "final"


@pytest.mark.parametrize("status", ["pending", "in_progress", None])
@pytest.mark.parametrize("following", ["message", "call"])
def test_nonterminal_or_missing_status_retains_cycle(
    persist_trajectory, status, following
):
    fields = {"status": status} if status is not None else {}
    events = [_text("Before"), _call("first"), _update("first", **fields)]
    events.append(_text(" after") if following == "message" else _call("second"))

    trajectory = persist_trajectory(events)

    assert len(trajectory.steps) == 2
    step = trajectory.steps[1]
    assert step.message == ("Before after" if following == "message" else "Before")
    assert _call_ids(step) == (
        ["first"] if following == "message" else ["first", "second"]
    )


@pytest.mark.parametrize(
    "statuses",
    [
        ("failed", "failed"),
        ("failed", "completed"),
        ("completed", "failed"),
        ("completed", "completed"),
    ],
)
def test_interleaved_calls_keep_ids_order_and_observations(
    persist_trajectory, statuses
):
    events = [
        _text("Try both"),
        _call("a"),
        _call("b"),
        _update("b", status="in_progress", rawOutput="b live"),
        _update("a", status=statuses[0], rawOutput="a final\n"),
        _update("a", status=statuses[0]),
        _update("b", status="in_progress", rawOutput="b later"),
        _update("b", status=statuses[1], rawOutput="b final"),
        _call("c"),
        _update("c", status="completed", rawOutput="c final"),
        _text("Done"),
    ]

    trajectory = persist_trajectory(events)

    assert len(trajectory.steps) == 4
    first, second, final = trajectory.steps[1:]
    assert _call_ids(first) == ["a", "b"]
    assert first.message == "Try both"
    assert [(r.source_call_id, r.content) for r in first.observation.results] == [
        ("a", "a final\n"),
        ("b", "b live\n\nb later\n\nb final"),
    ]
    assert _call_ids(second) == ["c"]
    assert second.observation.results[0].content == "c final"
    assert final.message == "Done"
    assert final.tool_calls is None


@pytest.mark.parametrize("exit_code", [0, 1, 7, 127])
def test_completed_invocation_preserves_shell_exit_code(persist_trajectory, exit_code):
    # ACP invocation completion is distinct from the nested process outcome.
    output = json.dumps({"status": "exited", "exit_code": exit_code, "stdout": "raw"})
    trajectory = persist_trajectory(
        [
            _call("shell"),
            _update("shell", status="completed", rawOutput=output),
            _text("Review output"),
        ]
    )

    assert len(trajectory.steps) == 3
    assert trajectory.steps[1].observation.results[0].content == output
    assert trajectory.steps[2].message == "Review output"
