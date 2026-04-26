"""Tests for :class:`CompositeArtifactBackend`.

Covers the local-first / bus-fallback read routing for every
ABC method, the local-only write routing, and the list-union
behavior (newest-first, dedup-by-id, limit cap).
"""

from __future__ import annotations

from typing import Any

import pytest

from store.artifacts import ArtifactBackend, ListResult
from store.composite import CompositeArtifactBackend


class _Recorder(ArtifactBackend):
    """Minimal stub that records every call and returns canned values.

    Each entry in ``responses`` is keyed by op name; a missing key
    returns the default error envelope so the test can verify the
    fall-through condition triggers correctly.
    """

    NOT_FOUND = {"error": "artifact not found"}

    def __init__(self, label: str, responses: dict[str, Any] | None = None) -> None:
        self.label = label
        self.responses = responses or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def _record(self, op: str, **kwargs: Any) -> Any:
        self.calls.append((op, kwargs))
        return self.responses.get(op, self.NOT_FOUND)

    async def list(self, **kwargs: Any) -> ListResult:
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
        return self._record("create", **kwargs)

    async def close(self) -> None:
        self.closed = True


# -- writes -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_writes_only_to_local():
    """Phase 4b's contract: writes never reach the fallback. The
    bus's `bus_artifact_*` endpoints are still reachable via the
    fallback for *reads*, but the local SQLite is the only
    destination for new artifacts.
    """
    local = _Recorder("local", {"create": {"id": "art_new"}})
    fallback = _Recorder("fallback")
    composite = CompositeArtifactBackend(local=local, fallback=fallback)

    result = await composite.create(kind="note", title="t", content="c")
    assert result == {"id": "art_new"}
    assert [op for op, _ in local.calls] == ["create"]
    assert fallback.calls == []


# -- read fall-through --------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_returns_local_when_present():
    local = _Recorder("local", {"metadata": {"id": "art_a", "kind": "note"}})
    fallback = _Recorder("fallback")
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.metadata("art_a")
    assert result == {"id": "art_a", "kind": "note"}
    # Fallback never queried — local hit short-circuits.
    assert fallback.calls == []


@pytest.mark.asyncio
async def test_metadata_falls_through_on_local_not_found():
    local = _Recorder("local")  # default: artifact not found
    fallback = _Recorder("fallback", {"metadata": {"id": "art_a", "kind": "log"}})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.metadata("art_a")
    assert result == {"id": "art_a", "kind": "log"}
    assert [op for op, _ in local.calls] == ["metadata"]
    assert [op for op, _ in fallback.calls] == ["metadata"]


@pytest.mark.asyncio
async def test_metadata_returns_fallback_not_found_when_both_miss():
    local = _Recorder("local")
    fallback = _Recorder("fallback")
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.metadata("art_x")
    assert result == {"error": "artifact not found"}


@pytest.mark.asyncio
async def test_metadata_does_not_fall_through_on_local_storage_error():
    """A non-not-found error from local (e.g. ``"local store
    error"`` from a sqlite3 failure) is authoritative — falling
    through would mask the local-side issue behind whatever the
    bus happened to return.
    """
    local = _Recorder("local", {"metadata": {"error": "local store error"}})
    fallback = _Recorder("fallback", {"metadata": {"id": "art_a"}})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.metadata("art_a")
    assert result == {"error": "local store error"}
    assert fallback.calls == []  # never queried


@pytest.mark.asyncio
async def test_get_falls_through_with_kwargs_threaded():
    """Falling through must thread the same offset/max_chars to
    the fallback — otherwise a partial read on the local side
    would silently re-fetch with default bounds via the bus.
    """
    local = _Recorder("local")
    fallback = _Recorder("fallback", {"get": {"text": "hello"}})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    await composite.get("art_a", offset=10, max_chars=50)
    assert local.calls == [("get", {"id": "art_a", "offset": 10, "max_chars": 50})]
    assert fallback.calls == [("get", {"id": "art_a", "offset": 10, "max_chars": 50})]


@pytest.mark.asyncio
async def test_each_read_method_falls_through_independently():
    """All six per-id read methods must follow the same
    fall-through policy — easy to forget one when wiring them
    up. ``list()`` has its own union-merge policy and is
    covered by the dedicated list tests below.
    """
    not_found_responses: dict[str, Any] = {}  # all return default not-found
    local = _Recorder("local", not_found_responses)
    fallback = _Recorder("fallback", {
        "metadata": {"id": "x"},
        "get": {"text": "x"},
        "head": {"text": "x"},
        "tail": {"text": "x"},
        "grep": {"matches": 1, "returned_matches": 1},
        "excerpt": {"text": "x"},
    })
    composite = CompositeArtifactBackend(local=local, fallback=fallback)

    await composite.metadata("art_a")
    await composite.get("art_a")
    await composite.head("art_a")
    await composite.tail("art_a")
    await composite.grep("art_a", pattern="needle")
    await composite.excerpt("art_a", start_line=1, end_line=5)

    local_ops = [op for op, _ in local.calls]
    fallback_ops = [op for op, _ in fallback.calls]
    assert local_ops == ["metadata", "get", "head", "tail", "grep", "excerpt"]
    # Every miss falls through to the fallback.
    assert fallback_ops == local_ops


# -- list union --------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_unions_local_first_then_fallback():
    local = _Recorder("local", {"list": [{"id": "art_local_1"}, {"id": "art_local_2"}]})
    fallback = _Recorder("fallback", {"list": [{"id": "art_bus_1"}, {"id": "art_bus_2"}]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=10)
    assert isinstance(result, list)
    ids = [item["id"] for item in result]
    # Local rows come first (they're authoritative once migrated).
    assert ids == ["art_local_1", "art_local_2", "art_bus_1", "art_bus_2"]


@pytest.mark.asyncio
async def test_list_dedups_by_id_with_local_winning():
    """An artifact present on both sides (post-migration) must
    show up exactly once, with the local-side row taking
    precedence.
    """
    local = _Recorder("local", {"list": [{"id": "art_a", "title": "from-local"}]})
    fallback = _Recorder("fallback", {"list": [
        {"id": "art_a", "title": "from-bus"},
        {"id": "art_b", "title": "bus-only"},
    ]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=10)
    assert isinstance(result, list)
    titles = {item["id"]: item["title"] for item in result}
    assert titles == {"art_a": "from-local", "art_b": "bus-only"}


@pytest.mark.asyncio
async def test_list_caps_total_at_limit():
    local = _Recorder("local", {"list": [{"id": f"art_l_{i}"} for i in range(5)]})
    fallback = _Recorder("fallback", {"list": [{"id": f"art_b_{i}"} for i in range(5)]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=7)
    assert isinstance(result, list)
    assert len(result) == 7


@pytest.mark.asyncio
async def test_list_clamps_limit_to_max_list_limit():
    """Caller-supplied limit > MAX_LIST_LIMIT must clamp,
    matching the underlying backends' policy. The composite
    used to forward the raw value; ``local[:limit]`` would
    still work but ``len(local) >= limit`` could short-circuit
    incorrectly.
    """
    from store.local_store import MAX_LIST_LIMIT
    local = _Recorder("local", {"list": [{"id": f"art_{i}"} for i in range(5)]})
    fallback = _Recorder("fallback", {"list": []})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    await composite.list(limit=MAX_LIST_LIMIT * 10)
    # Local was called with the clamped value, not the raw one.
    assert local.calls[0][1]["limit"] == MAX_LIST_LIMIT


@pytest.mark.asyncio
async def test_list_with_zero_limit_short_circuits():
    """``limit=0`` returns ``[]`` without round-tripping either
    backend. Matches LocalArtifactStore's policy: "is anything
    matching?" answer is empty, not a one-row best effort.
    """
    local = _Recorder("local", {"list": [{"id": "art_a"}]})
    fallback = _Recorder("fallback", {"list": [{"id": "art_b"}]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=0)
    assert result == []
    assert local.calls == []
    assert fallback.calls == []


@pytest.mark.asyncio
async def test_list_with_negative_limit_collapses_to_empty():
    """Negative limit used to slip through and produce
    ``local[:negative]`` (an end-trimmed slice). Clamp makes
    that an empty list, consistent with the limit=0 case.
    """
    local = _Recorder("local", {"list": [{"id": "art_a"}]})
    fallback = _Recorder("fallback", {"list": []})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=-1)
    assert result == []


@pytest.mark.asyncio
async def test_list_dedups_within_fallback_page():
    """If the fallback's own page contains duplicates of an id
    not on the local side, both copies must NOT be appended.
    The merge loop now updates ``seen_ids`` as it goes.
    """
    local = _Recorder("local", {"list": []})
    fallback = _Recorder("fallback", {"list": [
        {"id": "art_dup"},
        {"id": "art_dup"},  # duplicate within the fallback page
        {"id": "art_x"},
    ]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=10)
    ids = [item["id"] for item in result]
    assert ids == ["art_dup", "art_x"]


@pytest.mark.asyncio
async def test_list_skips_fallback_rows_with_missing_id():
    """A fallback row with a missing/empty id can't participate
    in dedup and shouldn't quietly leak into the result. Skip it.
    """
    local = _Recorder("local", {"list": []})
    fallback = _Recorder("fallback", {"list": [
        {"id": ""},
        {"id": "art_real"},
        {"title": "no-id"},
    ]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=10)
    ids = [item["id"] for item in result]
    assert ids == ["art_real"]


@pytest.mark.asyncio
async def test_list_overfetches_fallback_to_compensate_for_dups():
    """If the fallback's first ``limit`` rows are mostly
    duplicates of local-side ids, the merged result could end
    up under-filled. The composite asks the fallback for
    ``limit + len(local)`` (capped at 100) so the dedup pass
    has room to find unique rows.
    """
    local = _Recorder("local", {"list": [
        {"id": "art_local_1"},
        {"id": "art_local_2"},
    ]})
    # Fallback returns 5 rows; first 2 duplicate the local set.
    fallback = _Recorder("fallback", {"list": [
        {"id": "art_local_1"},  # duplicate
        {"id": "art_local_2"},  # duplicate
        {"id": "art_bus_3"},
        {"id": "art_bus_4"},
        {"id": "art_bus_5"},
    ]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=4)
    assert isinstance(result, list)
    ids = [item["id"] for item in result]
    # Local rows first; fallback contributes 2 unique rows so the
    # merged list reaches the limit of 4 (without the over-fetch
    # we'd only see 2 + 0 = 2 if the duplicates had occupied the
    # fallback's first ``limit`` slots).
    assert ids == ["art_local_1", "art_local_2", "art_bus_3", "art_bus_4"]
    # And the fallback was called with the over-fetched limit
    # (limit=4 + len(local)=2 = 6, capped at 100).
    fallback_call = fallback.calls[0]
    assert fallback_call[0] == "list"
    assert fallback_call[1]["limit"] == 6


@pytest.mark.asyncio
async def test_list_skips_fallback_when_local_already_filled_budget():
    """When the local side already returns ``limit`` rows, the
    composite must not waste a round trip on the bus — the
    fallback contributes only when the local side underfilled.
    """
    local = _Recorder("local", {"list": [{"id": f"art_l_{i}"} for i in range(5)]})
    fallback = _Recorder("fallback", {"list": [{"id": "art_b_1"}]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list(limit=5)
    assert isinstance(result, list)
    assert len(result) == 5
    assert fallback.calls == []  # fallback never queried


@pytest.mark.asyncio
async def test_list_surfaces_local_error_envelope_verbatim():
    local = _Recorder("local", {"list": {"error": "local store error"}})
    fallback = _Recorder("fallback", {"list": [{"id": "art_b"}]})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list()
    assert result == {"error": "local store error"}
    assert fallback.calls == []  # local errored — don't substitute fallback


@pytest.mark.asyncio
async def test_list_returns_local_when_fallback_errors():
    """A degraded view (only local rows) is more useful than
    masking the bus outage entirely. Listing should still
    succeed — the operator sees fewer rows; the fallback failure
    surfaces in the agent log via the bus-side warning.
    """
    local = _Recorder("local", {"list": [{"id": "art_local"}]})
    fallback = _Recorder("fallback", {"list": {"error": "bus unreachable"}})
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    result = await composite.list()
    assert result == [{"id": "art_local"}]


# -- close --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_closes_both_halves():
    local = _Recorder("local")
    fallback = _Recorder("fallback")
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    await composite.close()
    assert local.closed and fallback.closed


def test_public_accessors_return_halves():
    """The migration tooling needs the local + fallback halves
    to populate the local store. Public ``local`` / ``fallback``
    accessors keep that coupling explicit instead of forcing
    callers to reach into ``_local`` / ``_fallback``.
    """
    local = _Recorder("local")
    fallback = _Recorder("fallback")
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    assert composite.local is local
    assert composite.fallback is fallback


@pytest.mark.asyncio
async def test_close_continues_when_one_half_raises():
    """A wedged backend can't poison the other half's cleanup —
    shutdown must run both ``close()`` calls regardless of which
    one fails.
    """
    class _BoomOnClose(_Recorder):
        async def close(self) -> None:  # type: ignore[override]
            self.closed = True
            raise RuntimeError("boom")

    local = _BoomOnClose("local")
    fallback = _Recorder("fallback")
    composite = CompositeArtifactBackend(local=local, fallback=fallback)
    await composite.close()  # must not raise
    assert local.closed and fallback.closed
