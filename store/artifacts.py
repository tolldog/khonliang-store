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
import math
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


# The bus's REST ``/v1/artifacts`` endpoint clamps ``limit`` to this
# value; ``LocalArtifactStore`` mirrors it (re-exported there) and
# the composite shares it for over-fetch math. It also bounds how
# many rows the bus-backed path can over-fetch when a client-side
# ``metadata`` / ``since`` predicate is active — see ``list`` below.
MAX_LIST_LIMIT = 100


# ---------------------------------------------------------------------------
# Timeframe parsing — shared across the whole store so the ``since``
# cutoff has one definition (the skill handler normalizes the raw
# arg through it, the bus-backed client-side filter reuses it to
# parse each row's ``created_at``, and a future consumer's
# reading-list timeframe maps onto the same canonical epoch).
# ---------------------------------------------------------------------------


def _finite_epoch(ts: float) -> float:
    """Return ``ts`` if it's a usable epoch, else raise ``ValueError``.

    Rejects two ways a float can be a valid number yet a useless
    cutoff, both of which would otherwise raise deep inside the
    local store's ``datetime.fromtimestamp`` (in ``_epoch_to_iso``,
    *outside* the ``sqlite3.Error`` envelope) and crash the skill
    call instead of yielding a structured error:

    * non-finite — ``float("nan")`` / ``float("inf")`` parse cleanly
      but aren't meaningful timestamps;
    * out of range — a finite but absurd magnitude (``1e20``) is past
      what ``datetime`` can represent and raises
      ``OverflowError`` / ``OSError``.

    The representability probe uses the same ``fromtimestamp`` call
    the local store will make, so anything that survives here is
    safe to format downstream.
    """
    if not math.isfinite(ts):
        raise ValueError("since must be a finite epoch or ISO-8601 timestamp")
    try:
        datetime.fromtimestamp(ts, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("since is out of the representable timestamp range") from exc
    return ts


def parse_timestamp(value: Any) -> Optional[float]:
    """Coerce an epoch number or ISO-8601 string to epoch seconds (UTC).

    Returns ``None`` for ``None`` / empty input (no cutoff).
    Accepts:

    * ``int`` / ``float`` — already epoch seconds (``bool`` is
      rejected: ``True``/``False`` would silently become 1.0/0.0;
      ``nan`` / ``inf`` are rejected: a non-finite cutoff isn't a
      meaningful timestamp and would later overflow
      ``datetime.fromtimestamp`` in the local store).
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
        return _finite_epoch(float(value))
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Try ISO-8601 *first*. A basic-format date like
        # ``"20260301"`` is a valid ISO date but also parses through
        # ``float`` — if float ran first it'd be misread as the epoch
        # ``20260301.0`` (≈1970-08-22) and silently filter against
        # the wrong instant. Genuine epoch strings aren't ambiguous
        # the other way: a 10-digit integer, or any value with ``.``
        # / ``e``, is not valid ISO, so it falls through to the
        # numeric branch below. ``fromisoformat`` doesn't accept the
        # ``Z`` zone designator before 3.11, so swap it for ``+00:00``.
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            pass
        else:
            if dt.tzinfo is None:
                # Naive timestamp assumed UTC, matching how the local
                # store stamps ``created_at`` (SQLite ``'now'`` is UTC).
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        # Not ISO — treat it as a bare epoch number. ``float`` also
        # parses ``"nan"`` / ``"inf"`` — ``_finite_epoch`` rejects
        # those (and out-of-range magnitudes) so a useless cutoff
        # can't slip through.
        try:
            return _finite_epoch(float(s))
        except ValueError as exc:
            raise ValueError(
                "since must be an epoch number or ISO-8601 timestamp"
            ) from exc
    raise ValueError("since must be an epoch number or ISO-8601 timestamp")


def _scalar_eq(stored: Any, wanted: Any) -> bool:
    """Scalar equality that keeps booleans distinct from 0/1.

    ``bool`` is a subclass of ``int`` in Python, so ``True == 1``
    and ``False == 0``. Without a guard, a filter of ``{"final":
    True}`` would match a stored ``{"final": 1}`` (and vice versa),
    violating the documented scalar-equality contract. Treat a
    value as equal only when both sides agree on bool-ness, then
    compare normally (so ``1 == 1.0`` still holds for genuine
    numbers).
    """
    if isinstance(stored, bool) != isinstance(wanted, bool):
        return False
    return stored == wanted


def metadata_matches(item_metadata: Any, metadata_filter: dict[str, Any]) -> bool:
    """Subset/AND match used by client-side (bus) filtering.

    Returns ``True`` iff ``item_metadata`` is a dict that contains
    every ``key=value`` pair in ``metadata_filter`` (scalar
    equality on each, via :func:`_scalar_eq` so booleans don't
    collapse into 0/1). A key that's absent, or present with a
    differing value, fails the whole match. The plain ``dict``
    lookup here treats the key literally; that matches the local
    store's in-engine predicate, which quotes the key into the JSON
    path (``json_extract(metadata, '$."<key>"') = ?``) precisely so
    a dotted/bracketed key stays a literal member rather than a
    nested-path traversal.
    """
    if not isinstance(item_metadata, dict):
        return False
    for key, value in metadata_filter.items():
        if key not in item_metadata:
            return False
        if not _scalar_eq(item_metadata[key], value):
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
        # The bus REST surface can't push down the metadata / since
        # predicates, so they're applied client-side below. That
        # means we must NOT send the caller's (possibly small)
        # ``limit`` to the bus when a filter is active: the bus
        # would truncate the page to ``limit`` *before* filtering,
        # starving the filtered result and violating the contract
        # that ``limit`` bounds the *filtered* set. Over-fetch up
        # to the bus's clamp instead, filter, then slice to
        # ``limit``. When no filter is active, the page already is
        # the result, so pass ``limit`` through unchanged.
        #
        # Hard limitation: the bus list endpoint clamps to
        # ``MAX_LIST_LIMIT`` rows and exposes NO offset/cursor
        # (bus/artifacts.py::list — ``min(limit, 100)``, no
        # pagination), so the newest 100 rows are all this path can
        # ever see. On a bus-resident corpus larger than that, a
        # filtered match older than the 100th row is unreachable
        # here — we log a warning when the page is saturated, but
        # NORMALIZE limit first, before it drives either the bus
        # fetch or the client-side slice. Mirror the local /
        # composite ``[0, MAX_LIST_LIMIT]`` clamp so a negative
        # ``limit`` can't turn ``filtered[:limit]`` into an
        # "all-but-last-N" slice, and ``limit=0`` short-circuits to
        # an empty list (a "does anything match?" probe) instead of
        # the bus's ``max(1, …)`` bumping it back to one row.
        #
        # the only complete fix is the LocalArtifactStore in-SQL
        # predicate (the supported path; this bus proxy is
        # deprecated and slated for removal). The first consumer of
        # this filter — reviewer-persisted review artifacts — is
        # created via ``artifact_create``, which only works against
        # a write-capable backend (local / composite), so those rows
        # are never read through this bus-only path in practice.
        limit = max(0, min(int(limit), MAX_LIST_LIMIT))
        if limit == 0:
            return []
        filtering = bool(metadata) or since is not None
        fetch_limit = MAX_LIST_LIMIT if filtering else limit
        params = {
            "session_id": session_id,
            "kind": kind,
            "producer": producer,
            "limit": fetch_limit,
        }
        # is_id_route=False: a 404 here means the route is gone or
        # the bus_url is wrong, NOT "artifact not found" (that
        # translation only makes sense for per-id routes below).
        result = await self._get_json(
            "/v1/artifacts", params=params, is_id_route=False
        )
        if isinstance(result, list):
            # This is the deprecated read path; the scalable in-SQL
            # filter lives on LocalArtifactStore.
            filtered = apply_client_filters(result, metadata, since)
            if (
                filtering
                and len(filtered) < limit
                and len(result) >= MAX_LIST_LIMIT
            ):
                # The bus page hit its own clamp, so matches beyond
                # it are invisible: the filtered result may be
                # incomplete. Surface it rather than silently
                # returning a short page that looks exhaustive.
                logger.warning(
                    "bus-backed filtered list may be incomplete: page hit the "
                    "bus clamp (%d rows) with %d matches < limit %d "
                    "(metadata=%r, since=%r)",
                    MAX_LIST_LIMIT, len(filtered), limit, metadata, since,
                )
            return filtered[:limit]
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
