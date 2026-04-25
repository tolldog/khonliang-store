"""Tests for StoreAgent using the bus-lib testing harness.

Phase 1 (scaffold) shipped first; Phase 3 (viewer) added the
``display`` skill. health_check is still inherited from
:class:`BaseAgent`. This module covers registration shape, the
handler argument-parsing, and end-to-end ``display`` happy path
with a stubbed bus fetch.
"""

from __future__ import annotations

import pytest
from khonliang_bus.testing import AgentTestHarness

from store.agent import StoreAgent, _parse_artifact_refs
from store.viewer import ArtifactRef
from store.viewer import server as viewer_server


@pytest.fixture
def harness():
    return AgentTestHarness(StoreAgent)


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
async def test_display_handler_returns_url_session_and_tab_ids(harness):
    async def fake_request(*, agent_type, operation, args, **kwargs):
        assert agent_type == "bus"
        assert operation == "artifact_get"
        return {
            "result": {
                "text": "# Hello\n\nbody",
                "metadata": {"content_type": "text/markdown"},
            },
        }

    harness.agent.request = fake_request
    result = await harness.call("display", {
        "artifacts": '["art_aaa"]',
        "layout": "tabs",
    })
    assert "error" not in result
    assert result["session_id"]
    assert result["url"].endswith(f"/view/{result['session_id']}")
    assert len(result["tab_ids"]) == 1


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
async def test_display_handler_records_inline_error_on_fetch_failure(harness):
    async def boom(*, agent_type, operation, args, **kwargs):
        raise RuntimeError("bus down")

    harness.agent.request = boom
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
    session = server.registry.get_session(result["session_id"])
    assert session is not None
    only_tab = next(iter(session.tabs.values()))
    assert b"Failed to fetch artifact art_aaa" in only_tab.body
    assert only_tab.metadata.get("fetch_error") is True
