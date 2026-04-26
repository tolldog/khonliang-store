"""Tests for the artifact backend abstraction.

Covers :class:`BusBackedArtifactStore` end-to-end against an
``httpx.MockTransport`` so we can verify URL composition, query
parameters, and response shape without standing up a real bus.
"""

from __future__ import annotations

import json

import httpx
import pytest

from store.artifacts import BusBackedArtifactStore


def _make_backend(handler):
    """Construct a backend whose underlying client is wired to ``handler``."""
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    backend = BusBackedArtifactStore("http://bus.example/", client=client)
    return backend, client


@pytest.fixture
async def http_log():
    """Capture every (method, url, params) for assertion."""
    return []


@pytest.fixture
def make_handler(http_log):
    def _make(payload, *, status=200):
        body = json.dumps(payload).encode("utf-8") if not isinstance(payload, bytes) else payload

        def _handler(request: httpx.Request) -> httpx.Response:
            http_log.append({
                "method": request.method,
                "url": str(request.url),
                "path": request.url.path,
                # raw_path preserves percent-encoding so tests
                # asserting on URL-encoded ids see the wire form.
                "raw_path": request.url.raw_path.decode("ascii").split("?", 1)[0],
                "params": dict(request.url.params),
            })
            return httpx.Response(
                status,
                content=body,
                headers={"Content-Type": "application/json"},
            )

        return _handler

    return _make


# -- list ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_passes_filters_and_strips_trailing_slash(http_log, make_handler):
    backend, client = _make_backend(make_handler([{"id": "art_a"}]))
    try:
        result = await backend.list(
            session_id="s1", kind="tool_result", producer="dev", limit=5
        )
    finally:
        await client.aclose()
    assert result == [{"id": "art_a"}]
    call = http_log[0]
    # bus_url passed in with trailing '/' must be stripped — otherwise
    # we'd hit ``http://bus.example//v1/artifacts``.
    assert call["url"].startswith("http://bus.example/v1/artifacts")
    assert call["params"] == {
        "session_id": "s1",
        "kind": "tool_result",
        "producer": "dev",
        "limit": "5",
    }


@pytest.mark.asyncio
async def test_list_preserves_error_envelope_from_outage(make_handler):
    """A 5xx-via-_get_json must surface as an error envelope, not
    an empty list — empty-list would be indistinguishable from
    "no artifacts match these filters" and could mask outages.
    """
    backend, client = _make_backend(make_handler({}, status=500))
    try:
        result = await backend.list()
    finally:
        await client.aclose()
    assert result == {"error": "bus returned HTTP 500"}


@pytest.mark.asyncio
async def test_list_flags_unexpected_dict_response(make_handler):
    """Bus returns a dict that isn't a recognized error envelope
    (shouldn't happen but guard against silent miscoercion).
    """
    backend, client = _make_backend(make_handler({"weird": True}))
    try:
        result = await backend.list()
    finally:
        await client.aclose()
    assert result == {"error": "bus returned unexpected list shape"}


# -- metadata + per-id reads --------------------------------------------------


@pytest.mark.asyncio
async def test_id_with_reserved_chars_is_url_encoded(http_log, make_handler):
    """An artifact id containing reserved URL characters must be
    quoted as a single path segment so it can't escape the
    intended route. Today's bus ids are 12-char hex so this is
    defensive; locks in the contract for any future ID scheme.
    """
    backend, client = _make_backend(make_handler({}))
    try:
        # `/` would otherwise change /v1/artifacts/<id> →
        # /v1/artifacts/foo/bar — a different route entirely.
        await backend.metadata("foo/bar?q=1")
    finally:
        await client.aclose()
    # `quote(safe='')` percent-encodes everything that isn't [A-Za-z0-9_.-~].
    # Compare against raw_path because httpx decodes `path` for ergonomics.
    assert http_log[0]["raw_path"] == "/v1/artifacts/foo%2Fbar%3Fq%3D1"


@pytest.mark.asyncio
async def test_metadata_hits_artifact_path(http_log, make_handler):
    backend, client = _make_backend(make_handler({"id": "art_a", "size_bytes": 42}))
    try:
        meta = await backend.metadata("art_a")
    finally:
        await client.aclose()
    assert meta == {"id": "art_a", "size_bytes": 42}
    assert http_log[0]["path"] == "/v1/artifacts/art_a"


@pytest.mark.asyncio
async def test_get_threads_offset_and_max_chars(http_log, make_handler):
    backend, client = _make_backend(make_handler({"text": "hello"}))
    try:
        result = await backend.get("art_a", offset=120, max_chars=50)
    finally:
        await client.aclose()
    assert result == {"text": "hello"}
    assert http_log[0]["path"] == "/v1/artifacts/art_a/content"
    assert http_log[0]["params"] == {"offset": "120", "max_chars": "50"}


@pytest.mark.asyncio
async def test_head_and_tail(http_log, make_handler):
    backend, client = _make_backend(make_handler({"text": "..."}))
    try:
        await backend.head("art_a", lines=10, max_chars=200)
        await backend.tail("art_a", lines=15, max_chars=300)
    finally:
        await client.aclose()
    assert http_log[0]["path"] == "/v1/artifacts/art_a/head"
    assert http_log[0]["params"] == {"lines": "10", "max_chars": "200"}
    assert http_log[1]["path"] == "/v1/artifacts/art_a/tail"
    assert http_log[1]["params"] == {"lines": "15", "max_chars": "300"}


@pytest.mark.asyncio
async def test_grep_threads_pattern_and_caps(http_log, make_handler):
    backend, client = _make_backend(make_handler({"matches": []}))
    try:
        await backend.grep(
            "art_a",
            pattern="needle",
            context_lines=3,
            max_matches=5,
            max_chars=1000,
        )
    finally:
        await client.aclose()
    assert http_log[0]["path"] == "/v1/artifacts/art_a/grep"
    assert http_log[0]["params"] == {
        "pattern": "needle",
        "context_lines": "3",
        "max_matches": "5",
        "max_chars": "1000",
    }


@pytest.mark.asyncio
async def test_excerpt_threads_line_range(http_log, make_handler):
    backend, client = _make_backend(make_handler({"lines": []}))
    try:
        await backend.excerpt("art_a", start_line=10, end_line=25, max_chars=500)
    finally:
        await client.aclose()
    assert http_log[0]["path"] == "/v1/artifacts/art_a/excerpt"
    assert http_log[0]["params"] == {
        "start_line": "10",
        "end_line": "25",
        "max_chars": "500",
    }


# -- error handling -----------------------------------------------------------


@pytest.mark.asyncio
async def test_404_returns_artifact_not_found(make_handler):
    backend, client = _make_backend(make_handler({"detail": "x"}, status=404))
    try:
        result = await backend.metadata("art_missing")
    finally:
        await client.aclose()
    assert result == {"error": "artifact not found"}


@pytest.mark.asyncio
async def test_5xx_returns_error_envelope(make_handler):
    backend, client = _make_backend(make_handler({}, status=500))
    try:
        result = await backend.metadata("art_a")
    finally:
        await client.aclose()
    assert result == {"error": "bus returned HTTP 500"}


@pytest.mark.asyncio
async def test_connection_error_returns_unreachable_envelope():
    """Network failure must surface as an error envelope, not raise.

    The skill handlers expect a dict back from the backend so they
    can pass it straight through to the bus client. An uncaught
    exception would crash the handler.
    """
    def _boom(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nope")

    transport = httpx.MockTransport(_boom)
    client = httpx.AsyncClient(transport=transport)
    backend = BusBackedArtifactStore("http://bus.example", client=client)
    try:
        result = await backend.metadata("art_a")
    finally:
        await client.aclose()
    assert "error" in result
    assert "bus unreachable" in result["error"]


@pytest.mark.asyncio
async def test_non_json_body_returns_error_envelope():
    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<!doctype html>...", headers={"Content-Type": "text/html"})

    transport = httpx.MockTransport(_handler)
    client = httpx.AsyncClient(transport=transport)
    backend = BusBackedArtifactStore("http://bus.example", client=client)
    try:
        result = await backend.metadata("art_a")
    finally:
        await client.aclose()
    assert result == {"error": "bus returned non-JSON body"}


# -- lifecycle ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_only_closes_owned_client():
    """If the caller passes its own client, the backend must not close it."""
    transport = httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    external = httpx.AsyncClient(transport=transport)
    backend = BusBackedArtifactStore("http://bus.example", client=external)
    await backend.close()
    # The external client should still be usable.
    resp = await external.get("http://bus.example/v1/artifacts")
    assert resp.status_code == 200
    await external.aclose()
