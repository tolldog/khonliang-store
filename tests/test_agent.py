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

from store.agent import StoreAgent, _parse_artifact_refs
from store.artifacts import ArtifactBackend, ListResult
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

    async def list(self, **kwargs: Any) -> ListResult:
        # Annotation matches ``ArtifactBackend.list``: real
        # backends can return either the list of metadata dicts
        # or a single error-envelope dict on transport failure,
        # and the tests exercise both shapes.
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

    async def create(self, **kwargs: Any) -> dict[str, Any]:
        # Non-LocalArtifactStore backends raise from the ABC default;
        # the FakeBackend opts in by overriding so tests can exercise
        # the happy path without standing up SQLite. Tests that want
        # to see the read-only rejection use the default
        # BusBackedArtifactStore on the agent.
        return self._record("create", **kwargs)


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
    # round-trip — and asks for a viewer-sized window (well above
    # the read skills' 4000-char token-budget default) so a
    # normal-sized artifact renders in full. Asserting the cap is
    # "large" rather than the exact value lets us tune the
    # constant without churning this test.
    assert len(backend.calls) == 1
    op, kwargs = backend.calls[0]
    assert op == "get"
    assert kwargs["id"] == "art_aaa"
    assert kwargs["offset"] == 0
    assert kwargs["max_chars"] >= 1_000_000


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
    # Names the missing arg explicitly rather than the previous
    # combined "start_line and end_line are required" message —
    # easier to debug when only one is missing.
    assert result == {"error": "end_line is required"}
    assert backend.calls == []


@pytest.mark.asyncio
async def test_artifact_excerpt_rejects_non_integer_lines(harness, backend):
    result = await harness.call("artifact_excerpt", {
        "id": "art_a", "start_line": "abc", "end_line": 25,
    })
    assert result == {"error": "start_line must be an integer"}
    assert backend.calls == []


@pytest.mark.asyncio
async def test_int_args_reject_booleans(harness, backend):
    """``bool`` is a subclass of ``int`` in Python, so a JSON
    client typo (e.g., ``max_chars: true``) would otherwise
    silently coerce to ``max_chars=1`` and return very different
    content than the caller intended. Reject explicitly.
    """
    for op, args in [
        ("artifact_get", {"id": "art_a", "max_chars": True}),
        ("artifact_head", {"id": "art_a", "lines": False}),
        ("artifact_excerpt", {"id": "art_a", "start_line": True, "end_line": 10}),
    ]:
        result = await harness.call(op, args)
        assert "must be an integer" in result.get("error", ""), (
            f"{op} accepted a boolean: {result}"
        )


@pytest.mark.asyncio
async def test_int_args_reject_non_integer_floats(harness, backend):
    """``int(1.9)`` returns ``1`` silently, so ``offset=1.9`` would
    quietly become ``offset=1`` instead of erroring. Reject any
    float whose ``is_integer()`` is false.
    """
    result = await harness.call("artifact_get", {"id": "art_a", "offset": 1.9})
    assert result == {"error": "offset must be an integer"}
    assert backend.calls == []


@pytest.mark.asyncio
async def test_int_args_accept_integer_valued_floats(harness, backend):
    """JSON encoders can serialize ``1`` as ``1.0``; treating
    integer-valued floats as equivalent is friendlier than
    rejecting a wire-format quirk. ``1.5`` still gets rejected
    by the test above.
    """
    backend.response = {"text": ""}
    await harness.call("artifact_get", {"id": "art_a", "offset": 100.0})
    op, kwargs = backend.calls[0]
    assert kwargs["offset"] == 100
    assert isinstance(kwargs["offset"], int)


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


# -- artifact_create ----------------------------------------------------------


@pytest.mark.asyncio
async def test_artifact_create_threads_kwargs(harness, backend):
    backend.response = {"id": "art_xyz", "kind": "note"}
    result = await harness.call("artifact_create", {
        "kind": "note",
        "title": "Hello",
        "content": "Body",
        "metadata": {"k": "v"},
        "source_artifacts": ["art_a"],
        "id": "",
    })
    assert result == {"id": "art_xyz", "kind": "note"}
    op, kwargs = backend.calls[0]
    assert op == "create"
    assert kwargs["kind"] == "note"
    assert kwargs["title"] == "Hello"
    assert kwargs["content"] == "Body"
    assert kwargs["metadata"] == {"k": "v"}
    assert kwargs["source_artifacts"] == ["art_a"]
    # Empty caller-supplied id maps to "" (auto-generate); LocalArtifactStore
    # uses that signal to mint a fresh art_<hex> id.
    assert kwargs["artifact_id"] == ""


@pytest.mark.asyncio
async def test_artifact_create_validates_required_fields(harness, backend):
    cases = [
        ({}, "kind is required"),
        ({"kind": "note"}, "title is required"),
        ({"kind": "note", "title": "t"}, "content must be a string"),
        ({"kind": "note", "title": "t", "content": "x", "metadata": "not-an-object"}, "metadata must be an object"),
        ({"kind": "note", "title": "t", "content": "x", "source_artifacts": "not-a-list"}, "source_artifacts must be an array"),
        # Falsey-but-invalid types used to slip past
        # ``args.get("metadata") or {}`` since the falsey value
        # was coerced into the default before the type check.
        ({"kind": "note", "title": "t", "content": "x", "metadata": []}, "metadata must be an object"),
        ({"kind": "note", "title": "t", "content": "x", "source_artifacts": 0}, "source_artifacts must be an array"),
    ]
    for args, expected in cases:
        result = await harness.call("artifact_create", args)
        assert result == {"error": expected}, f"input={args!r}"
    # None of these should have reached the backend.
    assert backend.calls == []


@pytest.mark.asyncio
async def test_artifact_create_against_read_only_backend_returns_clear_error(harness):
    """Default ``BusBackedArtifactStore`` is read-only; the
    ABC's ``create`` raises ``NotImplementedError`` whose message
    names the backend. The handler must surface that as a clean
    error envelope rather than letting it propagate.
    """
    # No fixture here — keep the default BusBackedArtifactStore.
    result = await harness.call("artifact_create", {
        "kind": "note", "title": "t", "content": "c",
    })
    assert "error" in result
    assert "BusBackedArtifactStore" in result["error"]
    assert "read-only" in result["error"]


@pytest.mark.asyncio
async def test_advertises_artifact_create_skill(harness):
    skill_names = {s["name"] for s in harness.registration.skills}
    assert "artifact_create" in skill_names


# -- backend selection from config -------------------------------------------


def test_build_backend_defaults_to_bus(tmp_path):
    """Missing config or missing ``artifacts`` section yields a
    bus-backed backend so existing deployments keep working
    without a config change after the upgrade.
    """
    from store.agent import _build_backend

    # No config file at all.
    backend = _build_backend(config_path="", bus_url="http://bus")
    from store.artifacts import BusBackedArtifactStore
    assert isinstance(backend, BusBackedArtifactStore)


def test_build_backend_picks_local_when_configured(tmp_path):
    from store.agent import _build_backend
    from store.local_store import LocalArtifactStore

    cfg = tmp_path / "config.yaml"
    cfg.write_text("artifacts:\n  backend: local\n  db_path: store.db\n")
    backend = _build_backend(config_path=str(cfg), bus_url="http://bus")
    assert isinstance(backend, LocalArtifactStore)
    # Relative db_path resolves against config's directory so the
    # agent picks up the same DB regardless of cwd.
    assert backend._db_path == str(tmp_path / "store.db")


def test_build_backend_unknown_backend_falls_back_to_bus(tmp_path):
    """An unrecognized ``artifacts.backend`` value (typo, future
    backend kind, etc.) shouldn't take down agent startup —
    log a warning and use the safe default.
    """
    from store.agent import _build_backend
    from store.artifacts import BusBackedArtifactStore

    cfg = tmp_path / "config.yaml"
    cfg.write_text("artifacts:\n  backend: hypothetical\n")
    backend = _build_backend(config_path=str(cfg), bus_url="http://bus")
    assert isinstance(backend, BusBackedArtifactStore)


def test_build_backend_handles_non_string_config_values(tmp_path):
    """YAML happily decodes ``backend: 1`` as an int and
    ``db_path: ~`` as None. The previous code went straight to
    ``.strip().lower()`` on those and crashed at startup; now
    they're rejected at the config-load layer with a warning so
    the agent boots safely.
    """
    from store.agent import _build_backend
    from store.artifacts import BusBackedArtifactStore
    from store.local_store import LocalArtifactStore

    # Non-string backend value → fallback to bus.
    bad_backend = tmp_path / "bad-backend.yaml"
    bad_backend.write_text("artifacts:\n  backend: 1\n")
    backend = _build_backend(config_path=str(bad_backend), bus_url="http://bus")
    assert isinstance(backend, BusBackedArtifactStore)

    # Non-string db_path with backend=local → still picks
    # LocalArtifactStore but with the resolved default db path
    # (next to the config), not the malformed value.
    bad_db_path = tmp_path / "bad-db-path.yaml"
    bad_db_path.write_text("artifacts:\n  backend: local\n  db_path: []\n")
    backend2 = _build_backend(config_path=str(bad_db_path), bus_url="http://bus")
    assert isinstance(backend2, LocalArtifactStore)
    assert backend2._db_path == str(tmp_path / "store_artifacts.db")


# -- composite backend wiring ------------------------------------------------


def test_build_backend_composite_pairs_local_and_bus(tmp_path):
    """``backend: composite`` should construct a
    :class:`CompositeArtifactBackend` whose halves are a
    :class:`LocalArtifactStore` (for writes + local-first reads)
    and a :class:`BusBackedArtifactStore` (for read fallback).
    """
    from store.agent import _build_backend
    from store.composite import CompositeArtifactBackend
    from store.artifacts import BusBackedArtifactStore
    from store.local_store import LocalArtifactStore

    cfg = tmp_path / "config.yaml"
    cfg.write_text("artifacts:\n  backend: composite\n")
    backend = _build_backend(config_path=str(cfg), bus_url="http://bus")
    assert isinstance(backend, CompositeArtifactBackend)
    assert isinstance(backend._local, LocalArtifactStore)
    assert isinstance(backend._fallback, BusBackedArtifactStore)


# -- artifact_migrate_from_bus -----------------------------------------------


@pytest.mark.asyncio
async def test_migrate_requires_local_target(harness, backend):
    """The default ``BusBackedArtifactStore`` (read-only) has
    nowhere to copy *to*. The handler must surface that as a
    clear error rather than silently doing nothing.
    """
    # FakeBackend has no migration endpoints (not Local nor Composite).
    result = await harness.call("artifact_migrate_from_bus", {})
    assert "error" in result
    assert "local backend" in result["error"]


@pytest.mark.asyncio
async def test_migrate_requires_bus_fallback(harness, tmp_path):
    """``backend=local`` (no fallback) is similarly stuck —
    nothing to migrate from. Operator should switch to
    ``backend=composite``.
    """
    from store.local_store import LocalArtifactStore

    local = LocalArtifactStore(str(tmp_path / "local.db"))
    previous = harness.agent.set_backend(local)
    try:
        result = await harness.call("artifact_migrate_from_bus", {})
        assert "error" in result
        assert "composite" in result["error"]
    finally:
        await local.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_copies_artifacts_through_composite(harness, tmp_path):
    """End-to-end happy path: composite backend with a fake
    fallback that has two artifacts; artifact_migrate_from_bus
    pages through the list, fetches each artifact's content, and
    writes it to the local SQLite — all under the same id so
    callers see the same identifier under both backends.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "migration.db"))
    fallback_state = {
        "art_one": {"meta": {
            "id": "art_one", "kind": "note", "title": "First",
            "size_bytes": 5,
        }, "content": "hello"},
        "art_two": {"meta": {
            "id": "art_two", "kind": "log", "title": "Second",
            "size_bytes": 5,
        }, "content": "world"},
    }

    class _Bus(ArtifactBackend):
        async def list(self, **kw):
            return [v["meta"] for v in fallback_state.values()]
        async def metadata(self, aid):
            v = fallback_state.get(aid)
            return v["meta"] if v else {"error": "artifact not found"}
        async def get(self, aid, **kw):
            v = fallback_state.get(aid)
            if not v:
                return {"error": "artifact not found"}
            return {"artifact": v["meta"], "text": v["content"], "truncated": False}
        async def head(self, aid, **kw): return {"error": "artifact not found"}
        async def tail(self, aid, **kw): return {"error": "artifact not found"}
        async def grep(self, aid, **kw): return {"error": "artifact not found"}
        async def excerpt(self, aid, **kw): return {"error": "artifact not found"}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call("artifact_migrate_from_bus", {})
        assert result["copied"] == 2
        assert result["skipped"] == 0
        assert result["errors"] == []
        assert result["scanned"] == 2

        # Round-trip: post-migration metadata reads should hit
        # local-side rows (no fallback round trip needed).
        local_meta = await local.metadata("art_one")
        assert local_meta["title"] == "First"
        assert local_meta["id"] == "art_one"
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_is_idempotent(harness, tmp_path):
    """Re-running after a partial run must skip already-present
    ids rather than erroring on every duplicate-id INSERT. The
    skip count surfaces the resumption signal so operators see
    the run made progress.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "idempotent.db"))
    state = {
        "art_x": {"meta": {"id": "art_x", "kind": "note", "title": "X"}, "content": "x"},
    }

    class _Bus(ArtifactBackend):
        async def list(self, **kw): return [v["meta"] for v in state.values()]
        async def metadata(self, aid): return state[aid]["meta"]
        async def get(self, aid, **kw):
            return {"artifact": state[aid]["meta"], "text": state[aid]["content"]}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        first = await harness.call("artifact_migrate_from_bus", {})
        assert first["copied"] == 1 and first["skipped"] == 0

        # Re-run: every artifact is already present locally → all skipped.
        second = await harness.call("artifact_migrate_from_bus", {})
        assert second["copied"] == 0
        assert second["skipped"] == 1
        assert second["errors"] == []
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_dry_run_logs_without_writing(harness, tmp_path):
    """``dry_run=True`` exercises the same fetch path but never
    calls ``LocalArtifactStore.create``. The local DB stays
    empty; the report shows ``copied=N`` so the operator can
    confirm what a real run would do.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "dry.db"))
    state = {
        "art_y": {"meta": {"id": "art_y", "kind": "note", "title": "Y"}, "content": "y"},
    }

    class _Bus(ArtifactBackend):
        async def list(self, **kw): return [v["meta"] for v in state.values()]
        async def metadata(self, aid): return state[aid]["meta"]
        async def get(self, aid, **kw):
            return {"artifact": state[aid]["meta"], "text": state[aid]["content"]}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call("artifact_migrate_from_bus", {"dry_run": True})
        assert result["copied"] == 1
        assert result["dry_run"] is True
        # Local DB stays empty — nothing actually written.
        local_meta = await local.metadata("art_y")
        assert local_meta == {"error": "artifact not found"}
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_skill_advertised(harness):
    skill_names = {s["name"] for s in harness.registration.skills}
    assert "artifact_migrate_from_bus" in skill_names


@pytest.mark.asyncio
async def test_migrate_dry_run_reports_skipped_when_already_local(harness, tmp_path):
    """``dry_run`` must mirror the real run's outcome counts:
    when an id is already present locally, both modes should
    report ``skipped``. Previously dry-run reported ``copied``
    even for ids that would skip on a real run.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "dry-skip.db"))
    # Pre-populate locally so the migration would skip.
    await local.create(kind="note", title="X", content="x", artifact_id="art_x")

    state = {
        "art_x": {"meta": {"id": "art_x", "kind": "note", "title": "X"}, "content": "x"},
    }

    class _Bus(ArtifactBackend):
        async def list(self, **kw): return [v["meta"] for v in state.values()]
        async def metadata(self, aid): return state[aid]["meta"]
        async def get(self, aid, **kw):
            return {"artifact": state[aid]["meta"], "text": state[aid]["content"]}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call("artifact_migrate_from_bus", {"dry_run": True})
        assert result["copied"] == 0
        assert result["skipped"] == 1
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_records_truncation_as_error(harness, tmp_path):
    """If the source-side fetch came back truncated, the
    migration must NOT silently write partial content. The
    artifact ends up in ``errors`` instead of ``copied``.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "truncated.db"))

    class _Bus(ArtifactBackend):
        async def list(self, **kw):
            return [{"id": "art_big", "kind": "note", "title": "Big"}]
        async def metadata(self, aid):
            return {"id": "art_big", "kind": "note", "title": "Big"}
        async def get(self, aid, **kw):
            return {
                "artifact": {"id": "art_big"},
                "text": "partial-only",
                "truncated": True,
            }
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call("artifact_migrate_from_bus", {})
        assert result["copied"] == 0
        assert result["skipped"] == 0
        assert len(result["errors"]) == 1
        assert result["errors"][0]["error"] == "fetch truncated"
        # And the local DB should NOT have the partial row.
        meta = await local.metadata("art_big")
        assert meta == {"error": "artifact not found"}
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_records_per_artifact_error_on_local_metadata_failure(harness, tmp_path):
    """When the local-side ``metadata()`` returns an error other
    than ``"artifact not found"`` (e.g. ``"local store error"``
    from a sqlite failure), the migration must surface that as
    a per-artifact error rather than treating it as "not present"
    and proceeding to fetch + create. Prevents masking a real
    local-side issue with unnecessary work.
    """
    from store.composite import CompositeArtifactBackend

    class _SickLocal(ArtifactBackend):
        async def metadata(self, aid):
            return {"error": "local store error"}
        async def list(self, **kw): return []
        async def get(self, aid, **kw): return {"error": "local store error"}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}
        async def create(self, **kw):
            raise AssertionError(
                "create should not be reached when local metadata is sick"
            )

    class _Bus(ArtifactBackend):
        async def list(self, **kw):
            return [{"id": "art_x", "kind": "note", "title": "X"}]
        async def metadata(self, aid):
            return {"id": "art_x", "kind": "note", "title": "X"}
        async def get(self, aid, **kw):
            return {"artifact": {"id": "art_x"}, "text": "x"}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=_SickLocal(), fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call("artifact_migrate_from_bus", {})
        assert result["copied"] == 0
        assert result["skipped"] == 0
        assert len(result["errors"]) == 1
        assert "metadata failed" in result["errors"][0]["error"]
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_dry_run_accepts_string_booleans(harness, tmp_path):
    """``dry_run="false"`` used to slip past ``bool(value)`` and
    become ``True`` (any non-empty string is truthy in Python).
    The strict ``_bool_arg`` helper now treats common string
    forms (``"true"`` / ``"false"`` / ``"1"`` / ``"0"``) the
    way a human would expect.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "string-bool.db"))
    state = {
        "art_a": {"meta": {"id": "art_a", "kind": "note", "title": "A"}, "content": "a"},
    }

    class _Bus(ArtifactBackend):
        async def list(self, **kw): return [v["meta"] for v in state.values()]
        async def metadata(self, aid): return state[aid]["meta"]
        async def get(self, aid, **kw):
            return {"artifact": state[aid]["meta"], "text": state[aid]["content"]}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        # "false" → real run, not dry-run; the artifact actually
        # gets written.
        result = await harness.call(
            "artifact_migrate_from_bus", {"dry_run": "false"}
        )
        assert result["dry_run"] is False
        assert result["copied"] == 1
        # Local DB now has the row — proving the run wasn't
        # silently treated as dry.
        meta = await local.metadata("art_a")
        assert meta["title"] == "A"
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_dry_run_rejects_garbage_strings(harness, tmp_path):
    """Bogus values like ``dry_run="maybe"`` should fail loudly
    rather than be silently coerced.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "bogus-bool.db"))

    class _Bus(ArtifactBackend):
        async def list(self, **kw): return []
        async def metadata(self, aid): return {"error": "artifact not found"}
        async def get(self, aid, **kw): return {"error": "artifact not found"}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call(
            "artifact_migrate_from_bus", {"dry_run": "maybe"}
        )
        assert result == {"error": "dry_run must be a boolean"}
    finally:
        await composite.close()
        await previous.close()


@pytest.mark.asyncio
async def test_migrate_bus_list_failure_includes_dry_run(harness, tmp_path):
    """The skill contract advertises ``dry_run`` on every
    response shape; the bus-list-failure path must include it
    too so callers can rely on consistent keys.
    """
    from store.local_store import LocalArtifactStore
    from store.composite import CompositeArtifactBackend

    local = LocalArtifactStore(str(tmp_path / "list-fail.db"))

    class _Bus(ArtifactBackend):
        async def list(self, **kw): return {"error": "bus unreachable"}
        async def metadata(self, aid): return {"error": "artifact not found"}
        async def get(self, aid, **kw): return {"error": "artifact not found"}
        async def head(self, aid, **kw): return {}
        async def tail(self, aid, **kw): return {}
        async def grep(self, aid, **kw): return {}
        async def excerpt(self, aid, **kw): return {}

    composite = CompositeArtifactBackend(local=local, fallback=_Bus())
    previous = harness.agent.set_backend(composite)
    try:
        result = await harness.call("artifact_migrate_from_bus", {"dry_run": True})
        assert "error" in result
        assert "dry_run" in result
        assert result["dry_run"] is True
    finally:
        await composite.close()
        await previous.close()
