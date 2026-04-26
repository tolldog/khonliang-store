"""Tests for the SQLite-backed local artifact store.

Covers schema initialization, the ``create()`` write path, all
seven read operations against the same data, and edge cases the
bus's implementation already handles (size cap, regex grep,
line-range excerpts, list filters / limit caps).

Each test gets its own temp DB so they're isolation-safe.
"""

from __future__ import annotations

import pytest

from store.local_store import (
    LocalArtifactStore,
    MAX_ARTIFACT_BYTES,
    MAX_GREP_MATCHES,
    MAX_LIST_LIMIT,
)


@pytest.fixture
async def backend(tmp_path):
    """Per-test LocalArtifactStore against a fresh temp SQLite file.

    Async generator so we can ``await backend.close()`` in the
    teardown — closes the connection cleanly even when the test
    fails mid-flight.
    """
    db_path = str(tmp_path / "store.db")
    store = LocalArtifactStore(db_path)
    try:
        yield store
    finally:
        await store.close()


# -- create -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_returns_metadata_with_generated_id(backend):
    meta = await backend.create(
        kind="note", title="hello", content="hi there",
    )
    assert meta["kind"] == "note"
    assert meta["title"] == "hello"
    assert meta["id"].startswith("art_")
    # Wire compatibility with the bus: size_bytes counts UTF-8
    # bytes; sha256 is the hex digest of those same bytes.
    assert meta["size_bytes"] == 8
    assert meta["sha256"] == (
        # sha256("hi there"), computed once at test-write time so
        # this also catches accidental hashing-input changes.
        "9b96a1fe1d548cbbc960cc6a0286668fd74a763667b06366fb2324269fcabaa4"
    )


@pytest.mark.asyncio
async def test_create_preserves_caller_supplied_id(backend):
    meta = await backend.create(
        kind="note", title="t", content="c", artifact_id="art_custom",
    )
    assert meta["id"] == "art_custom"


@pytest.mark.asyncio
async def test_create_rejects_missing_kind_or_title(backend):
    assert (await backend.create(kind="", title="t", content="c")) == {
        "error": "kind is required"
    }
    assert (await backend.create(kind="note", title="", content="c")) == {
        "error": "title is required"
    }


@pytest.mark.asyncio
async def test_create_rejects_oversized_content(backend):
    big = "a" * (MAX_ARTIFACT_BYTES + 1)
    result = await backend.create(kind="note", title="t", content=big)
    assert "error" in result
    assert "exceeds maximum size" in result["error"]


@pytest.mark.asyncio
async def test_create_rejects_duplicate_id(backend):
    await backend.create(
        kind="note", title="t", content="c", artifact_id="art_dup",
    )
    second = await backend.create(
        kind="note", title="t", content="c", artifact_id="art_dup",
    )
    assert "error" in second
    assert "duplicate" in second["error"]


@pytest.mark.asyncio
async def test_duplicate_id_failure_doesnt_lock_subsequent_writes(backend):
    """The IntegrityError from a duplicate-id INSERT must trigger
    a rollback; otherwise sqlite3's implicit transaction stays
    aborted and the next write fails with "cannot operate on an
    aborted transaction" (or the DB stays locked under WAL).
    """
    await backend.create(kind="note", title="A", content="x", artifact_id="art_x")
    err = await backend.create(kind="note", title="B", content="y", artifact_id="art_x")
    assert "error" in err
    # Subsequent unrelated write must succeed — proves the
    # transaction was rolled back rather than left in the failed
    # state.
    next_ok = await backend.create(kind="note", title="C", content="z")
    assert next_ok.get("id", "").startswith("art_")
    assert next_ok["title"] == "C"


@pytest.mark.asyncio
async def test_create_persists_metadata_and_source_artifacts_as_json(backend):
    meta = await backend.create(
        kind="note", title="t", content="c",
        metadata={"source": "test", "n": 1},
        source_artifacts=["art_a", "art_b"],
    )
    # Round-tripped through SQLite TEXT columns + json decode.
    assert meta["metadata"] == {"source": "test", "n": 1}
    assert meta["source_artifacts"] == ["art_a", "art_b"]


@pytest.mark.asyncio
async def test_create_rejects_non_json_serializable_metadata(backend):
    """``json.dumps`` raises ``TypeError`` on unsupported types
    (datetime, bytes, set). The handler must catch that and
    return a clean envelope rather than letting the exception
    bubble up and crash the skill call.
    """
    import datetime
    result = await backend.create(
        kind="note", title="t", content="c",
        metadata={"when": datetime.datetime.now()},
    )
    assert "error" in result
    assert "JSON-serializable" in result["error"]


# -- metadata -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_returns_error_envelope_for_missing(backend):
    assert (await backend.metadata("art_nope")) == {"error": "artifact not found"}


@pytest.mark.asyncio
async def test_metadata_round_trips_after_create(backend):
    created = await backend.create(kind="note", title="t", content="c")
    fetched = await backend.metadata(created["id"])
    # Same id, kind, title, sha256 — created_at may differ in
    # representation but both are present.
    for field in ("id", "kind", "title", "sha256", "size_bytes"):
        assert created[field] == fetched[field], field


# -- list ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_newest_first(backend):
    a = await backend.create(kind="note", title="A", content="aaa")
    b = await backend.create(kind="note", title="B", content="bbb")
    items = await backend.list()
    # Reversed-insert order — list() emits newest first.
    assert isinstance(items, list)
    ids = [item["id"] for item in items]
    assert ids[0] == b["id"]
    assert ids[1] == a["id"]


@pytest.mark.asyncio
async def test_list_filters_by_kind_session_producer(backend):
    await backend.create(kind="note", title="A", content="x", session_id="s1")
    await backend.create(kind="log", title="B", content="x", session_id="s1")
    await backend.create(kind="note", title="C", content="x", session_id="s2", producer="agent-x")

    by_kind = await backend.list(kind="note")
    assert {a["title"] for a in by_kind} == {"A", "C"}

    by_session = await backend.list(session_id="s1")
    assert {a["title"] for a in by_session} == {"A", "B"}

    by_producer = await backend.list(producer="agent-x")
    assert {a["title"] for a in by_producer} == {"C"}


@pytest.mark.asyncio
async def test_list_clamps_limit_to_hard_max(backend):
    """Caller-supplied limit > MAX_LIST_LIMIT must clamp rather
    than honor — protects the agent from a runaway request that
    asks for the entire DB at once.
    """
    for i in range(5):
        await backend.create(kind="note", title=f"a-{i}", content="x")
    # Asking for 10x the hard cap must clamp; we don't have that
    # many rows, so just verify the request didn't blow up and
    # returned <= the cap.
    items = await backend.list(limit=MAX_LIST_LIMIT * 10)
    assert isinstance(items, list)
    assert len(items) <= MAX_LIST_LIMIT


@pytest.mark.asyncio
async def test_list_with_zero_limit_returns_empty(backend):
    """``limit=0`` is a "does anything match?" query — the
    answer must be ``[]``, not a one-row "best effort". Previous
    behavior clamped to 1 and returned a row the caller didn't
    ask for.
    """
    for i in range(3):
        await backend.create(kind="note", title=f"a-{i}", content="x")
    items = await backend.list(limit=0)
    assert items == []


# -- get ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_bounded_window(backend):
    created = await backend.create(
        kind="note", title="t", content="abcdefghij",
    )
    payload = await backend.get(created["id"], offset=2, max_chars=4)
    assert payload["text"] == "cdef"
    assert payload["truncated"] is True
    assert payload["artifact"]["id"] == created["id"]


@pytest.mark.asyncio
async def test_get_truncated_false_when_window_covers_remainder(backend):
    created = await backend.create(kind="note", title="t", content="abc")
    payload = await backend.get(created["id"], offset=0, max_chars=100)
    assert payload["text"] == "abc"
    assert payload["truncated"] is False


@pytest.mark.asyncio
async def test_get_missing_artifact_returns_error_envelope(backend):
    assert (await backend.get("art_nope")) == {"error": "artifact not found"}


# -- head / tail --------------------------------------------------------------


@pytest.mark.asyncio
async def test_head_returns_first_n_lines(backend):
    body = "\n".join(f"line {i}" for i in range(10))
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.head(created["id"], lines=3)
    assert payload["text"] == "line 0\nline 1\nline 2"
    assert payload["truncated"] is True


@pytest.mark.asyncio
async def test_tail_includes_line_range_metadata(backend):
    body = "\n".join(f"line {i}" for i in range(10))
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.tail(created["id"], lines=2)
    # Tail emits inclusive 1-indexed start_line/end_line so the
    # caller can locate the slice within the original artifact.
    assert payload["text"] == "line 8\nline 9"
    assert payload["start_line"] == 9
    assert payload["end_line"] == 10


@pytest.mark.asyncio
async def test_tail_with_zero_lines_signals_empty_via_zero_range(backend):
    """``lines=0`` selects nothing. Previously ``start_line``
    came back as ``len(content_lines)+1`` while ``end_line``
    stayed at ``len`` — start > end, broken invariant. Now both
    collapse to 0 to signal "no content" without violating the
    line-range contract.
    """
    body = "\n".join(f"line {i}" for i in range(5))
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.tail(created["id"], lines=0)
    assert payload["text"] == ""
    assert payload["start_line"] == 0
    assert payload["end_line"] == 0
    assert payload["start_line"] <= payload["end_line"]


# -- excerpt ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_excerpt_returns_inclusive_range(backend):
    body = "\n".join(f"line {i}" for i in range(10))
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.excerpt(created["id"], start_line=3, end_line=5)
    assert payload["text"] == "line 2\nline 3\nline 4"
    assert payload["start_line"] == 3
    assert payload["end_line"] == 5


@pytest.mark.asyncio
async def test_excerpt_past_eof_signals_empty_via_zero_range(backend):
    """Same line-range invariant as tail: when ``start_line`` is
    beyond the artifact's last line, the response previously
    reported ``start_line=<requested>`` and ``end_line=<len>``,
    yielding ``start > end``. Now both collapse to 0.
    """
    body = "line 0\nline 1\nline 2"  # 3 lines
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.excerpt(created["id"], start_line=10, end_line=20)
    assert payload["text"] == ""
    assert payload["start_line"] == 0
    assert payload["end_line"] == 0
    assert payload["start_line"] <= payload["end_line"]


# -- grep ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grep_returns_match_blocks(backend):
    body = "alpha\nfoo bar\nbeta\nfoo baz\ngamma"
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.grep(
        created["id"], pattern=r"foo", context_lines=0, max_matches=10,
    )
    assert payload["matches"] == 2
    assert payload["returned_matches"] == 2
    assert "foo bar" in payload["text"]
    assert "foo baz" in payload["text"]


@pytest.mark.asyncio
async def test_grep_caps_max_matches(backend):
    """Asking for more matches than ``MAX_GREP_MATCHES`` clamps;
    the request returns successfully rather than erroring so an
    overzealous caller still gets a useful answer.
    """
    body = "\n".join("hit" for _ in range(MAX_GREP_MATCHES + 5))
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.grep(
        created["id"],
        pattern=r"hit",
        max_matches=MAX_GREP_MATCHES * 10,
    )
    assert payload["returned_matches"] <= MAX_GREP_MATCHES
    # ``matches`` is the total hit count (not capped) so the
    # caller can tell their request was over-clamped.
    assert payload["matches"] == MAX_GREP_MATCHES + 5


@pytest.mark.asyncio
async def test_grep_invalid_regex_returns_error(backend):
    created = await backend.create(kind="note", title="t", content="abc")
    payload = await backend.grep(created["id"], pattern="[")
    assert "error" in payload
    assert "invalid regex" in payload["error"]


@pytest.mark.asyncio
async def test_grep_with_zero_max_matches_returns_count_only(backend):
    """``max_matches=0`` is a "count, don't return" query —
    response carries ``matches=N, returned_matches=0`` so the
    caller can probe for hits cheaply. Previous behavior clamped
    to 1 and returned the first match's content block whether
    the caller wanted it or not.
    """
    body = "alpha\nfoo\nbeta\nfoo\ngamma"
    created = await backend.create(kind="note", title="t", content=body)
    payload = await backend.grep(
        created["id"], pattern=r"foo", max_matches=0,
    )
    assert payload["matches"] == 2
    assert payload["returned_matches"] == 0
    assert payload["text"] == ""


# -- close --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_is_idempotent(tmp_path):
    """Repeated close() calls (e.g., shutdown re-runs after a
    failed start) must not raise. Locks in the contract used by
    ``StoreAgent.shutdown``.
    """
    store = LocalArtifactStore(str(tmp_path / "x.db"))
    # Force connection open.
    await store.create(kind="note", title="t", content="c")
    await store.close()
    await store.close()  # second call must be a no-op


# -- ABC create() default -----------------------------------------------------


@pytest.mark.asyncio
async def test_bus_backed_backend_rejects_create():
    """``BusBackedArtifactStore`` inherits the ABC's default
    create() which raises NotImplementedError. The error message
    names the backend so operators can diagnose a misconfigured
    deployment ("backend=bus, but agent_create called").
    """
    from store.artifacts import BusBackedArtifactStore

    backend = BusBackedArtifactStore("http://bus.example")
    try:
        with pytest.raises(NotImplementedError, match="BusBackedArtifactStore"):
            await backend.create(kind="note", title="t", content="c")
    finally:
        await backend.close()
