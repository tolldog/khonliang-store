"""Tests for StoreAgent using the bus-lib testing harness.

Phase 1 is scaffold-only. The agent declares no subclass skills of
its own and relies on :class:`BaseAgent`'s built-in ``health_check``
handler for liveness probes. Follow-up FRs will add artifact +
viewer surfaces; their tests will live alongside.

A note on what ``harness.registration.skills`` contains: the
:class:`AgentTestHarness` builds registration from the subclass's
``register_skills()``, which doesn't include :attr:`BaseAgent.BUILT_IN_SKILLS`.
So an empty list is the correct expectation for this phase — the
built-in ``health_check`` *handler* is still dispatchable via
``harness.call``, just not advertised through registration. When
Phase 2 lands with real skills, both checks grow together.
"""

from __future__ import annotations

import pytest
from khonliang_bus.testing import AgentTestHarness

from store.agent import StoreAgent


@pytest.fixture
def harness():
    return AgentTestHarness(StoreAgent)


def test_agent_type_is_store(harness):
    assert harness.agent.agent_type == "store"


def test_agent_id_defaults_to_store_test(harness):
    # AgentTestHarness uses ``f"{agent_type}-test"`` when no override.
    # This is just a sanity check that the default construction works.
    assert harness.agent.agent_id == "store-test"


def test_phase1_declares_no_subclass_skills(harness):
    reg = harness.registration
    assert reg.agent_type == "store"
    # Scaffold-only: no subclass skills yet. health_check lives in
    # BUILT_IN_SKILLS on BaseAgent and is dispatchable but not
    # advertised through register_skills(). This assertion is the
    # guard that tells us when the next phase landed without
    # updating the test.
    assert len(reg.skills) == 0


@pytest.mark.asyncio
async def test_health_check_is_dispatchable(harness):
    # harness.call routes via agent._handlers, which does include
    # the built-in health_check handler. Subclasses that don't
    # override it still get the default identity payload.
    result = await harness.call("health_check", {})
    assert isinstance(result, dict)
    assert result.get("agent_type") == "store"
    assert result.get("agent_id") == "store-test"
