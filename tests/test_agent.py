"""Tests for StoreAgent using the bus-lib testing harness.

Phase 1 (scaffold) → Phase 3 (viewer) → Phase 2 (artifact reads).
This module covers skill registration, handler argument-parsing,
the new artifact_* read handlers, and end-to-end ``display``
happy path with a stubbed backend.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from khonliang_bus.testing import AgentTestHarness

from store.agent import StoreAgent, _parse_artifact_refs, _VIEWER_FETCH_CAP_CHARS
from store.artifacts import ArtifactBackend
from store.viewer import ArtifactRef
from store.viewer import server as viewer_server


class FakeBackend(ArtifactBackend):
    """Test backend: returns canned values; records every call.

    Each method records ``(name, kwargs)`` into ``self.calls``;
    behavior knobs override individual responses (or raise to
    exercise error paths).
    """

    def __init__(self, response: Any = None) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.exc: Optional[BaseException] = None

    def _record(self, op: str, **kwargs: Any) -> Any:
        self.calls.append((op, kwargs))
        if self.exc is not None:
            raise self.exc
        return self.response

    async def list(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._record("list", **kwargs)

    async def metadata(self, artifact_id: str) -> dict[str, Any]:
        return self._record("metadata", id=artifact_id)

    async def get(self, artifact_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._record("get", id=artifact_id, **kwargs)

    async def head(self, artifact_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._record("head", id=artifact_id, **kwargs)

    async def tail(self, artifact_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._record("tail", id=artifact_id, **kwargs)

    async def grep(self, artifact_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._record("grep", id=artifact_id, **kwargs)

    async def excerpt(self, artifact_id: str, **kwargs: Any) -> dict[str, Any]:
        return self._record("excerpt", id=artifact_id, **kwargs)


@pytest.fixture
def harness():
    return AgentTestHarness(StoreAgent)


@pytest.fixture
async def backend(harness):
    """Swap in a FakeBackend; close the displaced default
    ``BusBackedArtifactStore`` on teardown so its httpx client
    doesn't leak across tests as ``ResourceWarning: unclosed
    client``.
    """
    fake = FakeBackend()
    previous = harness.agent.set_backend(fake)
    try:
        yield fake
    finally:
        await previous.close()


@pytest.fixture(autouse=True)
def _reset_viewer():
    """Drop the cached viewer singleton between tests so each one
    binds its own port and observes its own session state.
    """
    viewer_server._reset_for_tests()
    yield
    viewer_server._reset_for_tests()


def test_agent_type_is_store(harness):
    assert harness.agent.agent_type == "store"


def test_agent_id_defaults_to_store_test(harness):
    assert harness.agent.agent_id == "store-test"


def test_advertises_display_skill(harness):
    reg = harness.registration
    assert reg.agent_type == "store"
    skill_names = {s["name"] for s in reg.skills}
    assert "display" in skill_names


def test_handler_skill_consistency(harness):
    """Set-symmetry between Skill registrations and @handler methods.

    Mirrors the developer-side regression guard. Adding a skill
    without a handler (or vice versa) fails loudly.
    """
    skill_names = harness.all_skill_names
    handler_names = harness.handler_names
    assert skill_names == handler_names, (
        f"skills − handlers = {sorted(skill_names - handler_names)}; "
        f"handlers − skills = {sorted(handler_names - skill_names)}"
    )


@pytest.mark.asyncio
async def test_health_check_is_dispatchable(harness):
    result = await harness.call("health_check", {})
    assert isinstance(result, dict)
    assert result.get("agent_type") == "store"


# -- _parse_artifact_refs -----------------------------------------------------


def test_parse_artifact_refs_accepts_json_string_list():
    refs = _parse_artifact_refs('["art_a", "art_b"]')
    assert refs == [ArtifactRef(id="art_a"), ArtifactRef(id="art_b")]


def test_parse_artifact_refs_accepts_json_object_list():
    refs = _parse_artifact_refs(
        '[{"id": "art_a", "view_hint": "left"}, "art_b"]'
    )
    assert refs == [
        ArtifactRef(id="art_a", view_hint="left"),
        ArtifactRef(id="art_b"),
    ]


def test_parse_artifact_refs_accepts_comma_string():
    refs = _parse_artifact_refs("art_a, art_b ,art_c")
    assert refs == [
        ArtifactRef(id="art_a"),
        ArtifactRef(id="art_b"),
        ArtifactRef(id="art_c"),
    ]


def test_parse_artifact_refs_accepts_python_list():
    refs = _parse_artifact_refs([{"id": "art_a"}, "art_b"])
    assert refs == [ArtifactRef(id="art_a"), ArtifactRef(id="art_b")]


def test_parse_artifact_refs_rejects_object_without_id():
    with pytest.raises(ValueError, match="missing 'id'"):
        _parse_artifact_refs([{"view_hint": "left"}])


# -- display handler ----------------------------------------------------------


@pytest.mark.asyncio
async def test_display_handler_returns_url_session_and_tab_ids(harness, backend):
    backend.response = {
        "text": "# Hello\n\nbody",
        "metadata": {"content_type": "text/markdown"},
    }
    result = await harness.call("display", {
        "artifacts": '["art_aaa"]',
        "layout": "tabs",
    })
    assert "error" not in result
    assert result["session_id"]
    assert result["url"].endswith(f"/view/{result['session_id']}")
    assert len(result["tab_ids"]) == 1
    # Display routes through the backend in-process — no bus
    # round-trip — and asks for the viewer-sized cap (well above
    # the read skills' 4000-char token-budget default) so a
    # normal-sized artifact renders in full.
    assert backend.calls == [(
        "get",
        {"id": "art_aaa", "offset": 0, "max_chars": _VIEWER_FETCH_CAP_CHARS},
    )]


@pytest.mark.asyncio
async def test_display_handler_rejects_unimplemented_layout(harness):
    """Only 'tabs' is implemented today. 'split' is reserved for a
    follow-up FR and must be rejected with a clear error rather
    than silently accepted.
    """
    for bad in ("split", "carousel"):
        result = await harness.call("display", {
            "artifacts": '["art_aaa"]',
            "layout": bad,
        })
        assert "error" in result, f"layout={bad!r} should be rejected"
        assert bad in result["error"]
        assert "only 'tabs' is" in result["error"]


@pytest.mark.asyncio
async def test_display_handler_requires_artifacts(harness):
    result = await harness.call("display", {})
    assert "error" in result
    assert "artifacts" in result["error"].lower()


@pytest.mark.asyncio
async def test_display_handler_records_inline_error_on_fetch_failure(harness, backend):
    backend.exc = RuntimeError("bus down")
    result = await harness.call("display", {
        "artifacts": '["art_aaa"]',
    })
    # The session should still be created — fetch failures become
    # inline error tabs so callers see partial success.
    assert "error" not in result
    assert result["tab_ids"]
    # And the rendered tab body should describe the failure.
    server = viewer_server._SERVER
    assert server is not None
    snap = server.registry.session_snapshot(result["session_id"])
    assert snap is not None
    _, tabs = snap
    only_tab = tabs[0]
    assert b"Failed to fetch artifact art_aaa" in only_tab.body
    assert only_tab.metadata.get("fetch_error") is True


@pytest.mark.asyncio
async def test_display_handler_inlines_error_on_backend_error_envelope(harness, backend):
    """If the backend returns an error-envelope dict (rather than
    raising), the viewer must still treat it as a fetch failure
    and not silently render an empty tab.
    """
    backend.response = {"error": "artifact not found"}
    result = await harness.call("display", {"artifacts": '["art_missing"]'})
    assert "error" not in result
    server = viewer_server._SERVER
    assert server is not None
    snap = server.registry.session_snapshot(result["session_id"])
    assert snap is not None
    _, tabs = snap
    only_tab = tabs[0]
    assert b"Failed to fetch artifact art_missing" in only_tab.body
    assert b"artifact not found" in only_tab.body


# -- artifact read skill registrations ----------------------------------------


def test_advertises_all_phase2_read_skills(harness):
    """All seven read skills + display + health_check live on the
    skill list; if a handler-vs-skill drift is introduced the
    set-symmetry test below also catches it.
    """
    skill_names = {s["name"] for s in harness.registration.skills}
    expected = {
        "artifact_list", "artifact_metadata", "artifact_get",
        "artifact_head", "artifact_tail", "artifact_grep",
        "artifact_excerpt", "display",
    }
    assert expected.issubset(skill_names)


# -- artifact_list ------------------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_list_threads_filters(harness, backend):
    backend.response = [{"id": "art_a"}, {"id": "art_b"}]
    result = await harness.call("artifact_list", {
        "session_id": "s1", "kind": "tool_result", "producer": "dev", "limit": 5,
    })
    assert result == {"artifacts": [{"id": "art_a"}, {"id": "art_b"}]}
    assert backend.calls == [(
        "list",
        {"session_id": "s1", "kind": "tool_result", "producer": "dev", "limit": 5},
    )]


@pytest.mark.asyncio
async def test_artifact_list_uses_defaults_when_args_missing(harness, backend):
    backend.response = []
    await harness.call("artifact_list", {})
    op, kwargs = backend.calls[0]
    assert op == "list"
    assert kwargs["limit"] == 20
    assert kwargs["session_id"] == ""


@pytest.mark.asyncio
async def test_artifact_list_passes_error_envelope_through(harness, backend):
    """Backend can emit an error envelope (`{'error': ...}`) on
    network/5xx; the handler must pass it through unwrapped, the
    same way the other read skills do, so callers that check
    ``result.get('error')`` see the failure rather than a
    corrupted ``{'artifacts': {'error': ...}}`` shape.
    """
    backend.response = {"error": "bus returned HTTP 500"}
    result = await harness.call("artifact_list", {})
    assert result == {"error": "bus returned HTTP 500"}


# -- artifact_metadata --------------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_metadata_pulls_id_through(harness, backend):
    backend.response = {"id": "art_a", "size_bytes": 42}
    result = await harness.call("artifact_metadata", {"id": "art_a"})
    assert result == {"id": "art_a", "size_bytes": 42}
    assert backend.calls == [("metadata", {"id": "art_a"})]


@pytest.mark.asyncio
async def test_artifact_metadata_rejects_missing_id(harness, backend):
    result = await harness.call("artifact_metadata", {})
    assert result == {"error": "id is required"}
    assert backend.calls == []


# -- artifact_get -------------------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_get_threads_offset_and_max_chars(harness, backend):
    backend.response = {"text": "hello"}
    await harness.call("artifact_get", {"id": "art_a", "offset": 50, "max_chars": 200})
    assert backend.calls == [(
        "get",
        {"id": "art_a", "offset": 50, "max_chars": 200},
    )]


@pytest.mark.asyncio
async def test_artifact_get_coerces_string_numerics(harness, backend):
    """Bus clients sometimes pass numeric args as strings (JSON
    payload origin); the handler should coerce rather than reject.
    """
    backend.response = {"text": ""}
    await harness.call("artifact_get", {"id": "art_a", "offset": "100", "max_chars": "50"})
    op, kwargs = backend.calls[0]
    assert kwargs == {"id": "art_a", "offset": 100, "max_chars": 50}


@pytest.mark.asyncio
async def test_artifact_get_rejects_bad_offset(harness, backend):
    """Provided-but-non-numeric ints are an explicit error rather
    than a silent fallback to default. Silent fallback would mean
    ``offset='abc'`` becomes ``offset=0`` and starts returning
    different content than the caller asked for.
    """
    result = await harness.call("artifact_get", {"id": "art_a", "offset": "abc"})
    assert result == {"error": "offset must be an integer"}
    assert backend.calls == []


@pytest.mark.asyncio
async def test_artifact_grep_rejects_bad_max_chars(harness, backend):
    result = await harness.call("artifact_grep", {
        "id": "art_a", "pattern": "needle", "max_chars": "wat",
    })
    assert result == {"error": "max_chars must be an integer"}
    assert backend.calls == []


# -- artifact_head / tail -----------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_head_and_tail(harness, backend):
    backend.response = {"text": ""}
    await harness.call("artifact_head", {"id": "art_a", "lines": 10})
    await harness.call("artifact_tail", {"id": "art_a", "lines": 5, "max_chars": 100})
    assert backend.calls[0] == ("head", {"id": "art_a", "lines": 10, "max_chars": 4000})
    assert backend.calls[1] == ("tail", {"id": "art_a", "lines": 5, "max_chars": 100})


# -- artifact_grep ------------------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_grep_requires_pattern(harness, backend):
    result = await harness.call("artifact_grep", {"id": "art_a"})
    assert result == {"error": "pattern is required"}
    assert backend.calls == []


@pytest.mark.asyncio
async def test_artifact_grep_threads_caps(harness, backend):
    backend.response = {"matches": []}
    await harness.call("artifact_grep", {
        "id": "art_a", "pattern": "needle",
        "context_lines": 3, "max_matches": 5, "max_chars": 1000,
    })
    assert backend.calls == [(
        "grep",
        {"id": "art_a", "pattern": "needle", "context_lines": 3, "max_matches": 5, "max_chars": 1000},
    )]


# -- artifact_excerpt ---------------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_excerpt_requires_line_range(harness, backend):
    result = await harness.call("artifact_excerpt", {"id": "art_a", "start_line": 5})
    assert result == {"error": "start_line and end_line are required"}
    assert backend.calls == []


@pytest.mark.asyncio
async def test_artifact_excerpt_rejects_non_integer_lines(harness, backend):
    result = await harness.call("artifact_excerpt", {
        "id": "art_a", "start_line": "abc", "end_line": "xyz",
    })
    assert "error" in result
    assert "integers" in result["error"]
    assert backend.calls == []


@pytest.mark.asyncio
async def test_artifact_excerpt_threads_line_range(harness, backend):
    backend.response = {"lines": []}
    await harness.call("artifact_excerpt", {
        "id": "art_a", "start_line": 10, "end_line": 25, "max_chars": 500,
    })
    assert backend.calls == [(
        "excerpt",
        {"id": "art_a", "start_line": 10, "end_line": 25, "max_chars": 500},
    )]


# -- error envelope passthrough -----------------------------------------------


@pytest.mark.asyncio
async def test_read_skills_pass_through_backend_error_envelope(harness, backend):
    """When the backend returns ``{'error': ...}`` (e.g. 404 from
    bus or non-JSON response) the handler must surface it
    verbatim, not swallow it. Lets MCP / bus clients treat it
    the same way they'd handle a direct bus.artifact_metadata
    error envelope.
    """
    backend.response = {"error": "artifact not found"}
    result = await harness.call("artifact_metadata", {"id": "art_missing"})
    assert result == {"error": "artifact not found"}


# -- backend lifecycle --------------------------------------------------------


def test_set_backend_returns_previous(harness):
    """Caller can recover the previous backend and dispose of it
    on its own schedule (we deliberately don't auto-close).
    """
    initial = harness.agent._backend
    fake = FakeBackend()
    returned = harness.agent.set_backend(fake)
    assert returned is initial
    assert harness.agent._backend is fake


@pytest.mark.asyncio
async def test_shutdown_closes_owned_backend(harness):
    """``BusBackedArtifactStore`` owns an httpx client; shutdown
    must close it so the process doesn't emit unclosed-client
    warnings on exit.
    """
    closed = {"flag": False}

    class CloseRecording(FakeBackend):
        async def close(self) -> None:  # type: ignore[override]
            closed["flag"] = True

    harness.agent.set_backend(CloseRecording())
    # BaseAgent.shutdown disconnects the websocket and closes
    # internal state. The harness leaves the connector unstarted
    # so we patch _connector to None to make shutdown a no-op
    # apart from the backend close path under test.
    harness.agent._connector = None
    await harness.agent.shutdown()
    assert closed["flag"] is True


@pytest.mark.asyncio
async def test_shutdown_swallows_backend_close_errors(harness, caplog):
    """A backend.close() raising shouldn't take down agent
    shutdown — the rest of the teardown still needs to run.
    """
    class RaiseOnClose(FakeBackend):
        async def close(self) -> None:  # type: ignore[override]
            raise RuntimeError("backend wedged")

    harness.agent.set_backend(RaiseOnClose())
    harness.agent._connector = None
    await harness.agent.shutdown()  # must not raise
