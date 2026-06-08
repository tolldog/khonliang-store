"""Artifact backend abstraction.

The store agent's read skills (``artifact_get`` and friends) all
route through an :class:`ArtifactBackend`. Phase 2 ships
:class:`BusBackedArtifactStore` — an HTTP client that talks to the
bus's existing REST surface (where artifact data lives today).
Phase 4 will add a local SQLite-backed implementation that owns
the data; the skill surface and the viewer's fetch path don't
change when the swap happens.

Why HTTPX-to-bus-REST and not a websocket-routed
``request(agent_type='bus')`` call: the bus does not register
itself as an agent in the dispatch table today — that's separate
architectural debt Phase 4 inherits. The MCP adapter has
talked HTTP to ``/v1/artifacts/...`` since the bus shipped, so
HTTP is the established transport for the bus-as-data-source
relationship; preserving that shape keeps Phase 2 self-contained.
"""

from __future__ import annotations

import abc
import logging
from datetime import datetime, timezone
from typing import Any, Optional, Union
from urllib.parse import quote

import httpx


logger = logging.getLogger(__name__)


# Returned by list() when the underlying request errors. The bus's
# canonical list endpoint emits a list on success, so a dict with
# an ``error`` key is the unambiguous failure shape and lets
# callers distinguish "outage" from "zero artifacts".
ListResult = Union[list[dict[str, Any]], dict[str, Any]]


# ---------------------------------------------------------------------------
# Timeframe parsing — shared across the whole store so the ``since``
# cutoff has one definition (the skill handler normalizes the raw
# arg through it, the bus-backed client-side filter reuses it to
# parse each row's ``created_at``, and a future consumer's
# reading-list timeframe maps onto the same canonical epoch).
# ---------------------------------------------------------------------------


def parse_timestamp(value: Any) -> Optional[float]:
    """Coerce an epoch number or ISO-8601 string to epoch seconds (UTC).

    Returns ``None`` for ``None`` / empty input (no cutoff).
    Accepts:

    * ``int`` / ``float`` — already epoch seconds (``bool`` is
      rejected: ``True``/``False`` would silently become 1.0/0.0).
    * a numeric string (``"1780870948"``) — parsed as epoch.
    * an ISO-8601 string (``"2026-06-07T10:30:00Z"`` or naive
      ``"2026-06-07"``). A trailing ``Z`` is honored; a naive
      timestamp is assumed UTC to match how the local store stamps
      ``created_at`` (SQLite ``'now'`` is UTC).

    Raises ``ValueError`` on any other shape so the skill handler
    can surface a structured error rather than silently dropping
    the filter.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("since must be an epoch number or ISO-8601 timestamp")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # A bare numeric string is epoch seconds.
        try:
            return float(s)
        except ValueError:
            pass
        # Otherwise treat it as ISO-8601. ``fromisoformat`` doesn't
        # accept the ``Z`` zone designator before 3.11, so swap it
        # for the explicit ``+00:00`` offset.
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError as exc:
            raise ValueError(
                "since must be an epoch number or ISO-8601 timestamp"
            ) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    raise ValueError("since must be an epoch number or ISO-8601 timestamp")


def metadata_matches(item_metadata: Any, metadata_filter: dict[str, Any]) -> bool:
    """Subset/AND match used by client-side (bus) filtering.

    Returns ``True`` iff ``item_metadata`` is a dict that contains
    every ``key=value`` pair in ``metadata_filter`` (scalar
    equality on each). A key that's absent, or present with a
    differing value, fails the whole match. Mirrors the SQL
    ``json_extract(metadata, '$.<key>') = ?`` clauses the local
    store applies in-engine.
    """
    if not isinstance(item_metadata, dict):
        return False
    for key, value in metadata_filter.items():
        if key not in item_metadata:
            return False
        if item_metadata[key] != value:
            return False
    return True


def _created_after(created_at: Any, since_epoch: float) -> bool:
    """``True`` iff ``created_at`` parses and is >= ``since_epoch``.

    Tolerant: a missing / unparseable ``created_at`` excludes the
    row rather than raising, so one malformed timestamp can't fail
    a whole list response.
    """
    if not created_at:
        return False
    try:
        ts = parse_timestamp(created_at)
    except ValueError:
        return False
    return ts is not None and ts >= since_epoch


def apply_client_filters(
    items: list[dict[str, Any]],
    metadata_filter: Optional[dict[str, Any]],
    since: Optional[float],
) -> list[dict[str, Any]]:
    """Filter a fetched page of artifact rows by metadata / since.

    Used by read-only backends (the deprecated bus REST surface)
    that can't push the predicate down to the data source. Note:
    this narrows the *already-paged* result, so a match beyond the
    fetched page is invisible — the scalable path is the local
    store's in-SQL filter. Acceptable for the bus fallback, which
    is slated for removal once all operators have migrated.
    """
    if not metadata_filter and since is None:
        return items
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if metadata_filter and not metadata_matches(
            item.get("metadata"), metadata_filter
        ):
            continue
        if since is not None and not _created_after(item.get("created_at"), since):
            continue
        out.append(item)
    return out


def _encode_id(artifact_id: str) -> str:
    """Quote an artifact id as a single URL path segment.

    Bus artifact ids are 12-byte hex today, so no quoting is
    strictly necessary; encoding defensively means a future ID
    scheme that includes ``/`` or ``?`` (or a malicious caller
    that synthesizes one) can't change the requested route.
    """
    return quote(artifact_id, safe="")


class ArtifactBackend(abc.ABC):
    """Source-of-truth for artifact reads.

    All methods are async so the store agent's skill handlers can
    await them on the agent event loop without blocking. The
    return shape mirrors what the bus emits today; consumers can
    target either backend without a translation layer.
    """

    @abc.abstractmethod
    async def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
        metadata: Optional[dict[str, Any]] = None,
        since: Optional[float] = None,
    ) -> ListResult:
        """List artifact metadata, newest first.

        ``metadata`` is an optional subset/AND filter: only
        artifacts whose stored metadata contains every given
        ``key=value`` pair (scalar equality) are returned. ``since``
        is an optional ``created_at`` cutoff in epoch seconds
        (inclusive). Both compose (AND) with ``session_id`` / ``kind``
        / ``producer`` and with ``limit``, which bounds the *filtered*
        set. Absent ``metadata`` / ``since`` reproduces the
        pre-filter behavior exactly.
        """
        ...

    @abc.abstractmethod
    async def metadata(self, artifact_id: str) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def get(
        self,
        artifact_id: str,
        *,
        offset: int = 0,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def head(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def tail(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    @abc.abstractmethod
    async def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = 4000,
    ) -> dict[str, Any]: ...

    async def create(
        self,
        *,
        kind: str,
        title: str,
        content: str,
        producer: str = "",
        session_id: str = "",
        trace_id: str = "",
        content_type: str = "text/plain",
        metadata: Optional[dict[str, Any]] = None,
        source_artifacts: Optional[list[str]] = None,
        artifact_id: str = "",
        ttl: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a new artifact.

        Default raises ``NotImplementedError`` so read-only
        backends (today: :class:`BusBackedArtifactStore`) don't
        need an explicit override. Concrete write-capable
        backends — currently only :class:`LocalArtifactStore` —
        override with the persisting implementation. Returns the
        new artifact's metadata on success, or
        ``{"error": ...}`` on validation failure (size cap,
        duplicate id, etc.).
        """
        raise NotImplementedError(
            f"{type(self).__name__} is read-only; create not supported"
        )

    async def close(self) -> None:
        """Release resources owned by this backend.

        Default no-op for stateless backends; concrete backends
        that hold connections / file handles override. Declared
        on the ABC as `async` so :meth:`StoreAgent.shutdown` can
        ``await`` it without dynamic-attribute gymnastics or risk
        of finding a sync override.
        """


class BusBackedArtifactStore(ArtifactBackend):
    """Read artifacts from the bus's REST surface over HTTP.

    Each call hits ``GET /v1/artifacts[/...]`` on the bus URL the
    store agent was configured with. A single :class:`httpx.AsyncClient`
    is reused for connection pooling; the caller owns lifecycle via
    :meth:`close`.
    """

    # 30s is generous for a single-artifact read against a local
    # bus; tune downward if a remote bus is ever in scope.
    DEFAULT_TIMEOUT = 30.0

    def __init__(
        self,
        bus_url: str,
        *,
        client: Optional[httpx.AsyncClient] = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        # Strip trailing slash so f"{bus_url}/v1/..." doesn't double up.
        self._bus_url = bus_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # -- ArtifactBackend ----------------------------------------------------

    async def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
        metadata: Optional[dict[str, Any]] = None,
        since: Optional[float] = None,
    ) -> ListResult:
        params = {
            "session_id": session_id,
            "kind": kind,
            "producer": producer,
            "limit": limit,
        }
        # is_id_route=False: a 404 here means the route is gone or
        # the bus_url is wrong, NOT "artifact not found" (that
        # translation only makes sense for per-id routes below).
        result = await self._get_json(
            "/v1/artifacts", params=params, is_id_route=False
        )
        if isinstance(result, list):
            # The bus REST surface doesn't push down metadata /
            # since predicates, so apply them client-side over the
            # fetched page. This is the deprecated read path; the
            # scalable in-SQL filter lives on LocalArtifactStore.
            return apply_client_filters(result, metadata, since)
        # Preserve error envelopes (network failure / 4xx / 5xx /
        # non-JSON) so a transport blip is distinguishable from a
        # genuine "zero artifacts match these filters" result.
        if isinstance(result, dict) and "error" in result:
            return result
        return {"error": "bus returned unexpected list shape"}

    async def metadata(self, artifact_id: str) -> dict[str, Any]:
        return await self._get_json(f"/v1/artifacts/{_encode_id(artifact_id)}")

    async def get(
        self,
        artifact_id: str,
        *,
        offset: int = 0,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{_encode_id(artifact_id)}/content",
            params={"offset": offset, "max_chars": max_chars},
        )

    async def head(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{_encode_id(artifact_id)}/head",
            params={"lines": lines, "max_chars": max_chars},
        )

    async def tail(
        self,
        artifact_id: str,
        *,
        lines: int = 80,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{_encode_id(artifact_id)}/tail",
            params={"lines": lines, "max_chars": max_chars},
        )

    async def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{_encode_id(artifact_id)}/grep",
            params={
                "pattern": pattern,
                "context_lines": context_lines,
                "max_matches": max_matches,
                "max_chars": max_chars,
            },
        )

    async def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/artifacts/{_encode_id(artifact_id)}/excerpt",
            params={
                "start_line": start_line,
                "end_line": end_line,
                "max_chars": max_chars,
            },
        )

    # -- internals ----------------------------------------------------------

    async def _get_json(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        is_id_route: bool = True,
    ) -> Any:
        """GET ``path`` and decode the JSON body.

        ``is_id_route``: True for paths that target a specific
        artifact id (where 404 means "artifact not found"). False
        for collection / health routes where 404 means "bus
        misconfigured" — those fall through to the generic HTTP
        error envelope so callers don't see a misleading "artifact
        not found" for a missing-route problem.
        """
        url = f"{self._bus_url}{path}"
        try:
            resp = await self._client.get(url, params=params or {})
        except httpx.HTTPError as exc:
            # Log the full exception (with hostname / cause) for
            # operational diagnostics; the response envelope keeps
            # to a stable string so internal connection details
            # don't leak through to bus clients. ``exc_info=True``
            # captures the traceback under our logger; the WARNING
            # level matches "transient bus blip" rather than
            # "agent broken".
            logger.warning(
                "bus unreachable on GET %s: %s: %s",
                path, type(exc).__name__, exc,
                exc_info=True,
            )
            return {"error": "bus unreachable"}
        if resp.status_code == 404 and is_id_route:
            return {"error": "artifact not found"}
        if resp.status_code >= 400:
            return {"error": f"bus returned HTTP {resp.status_code}"}
        try:
            return resp.json()
        except ValueError:
            return {"error": "bus returned non-JSON body"}
