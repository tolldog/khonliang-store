"""Local SQLite-backed artifact store.

Owns the data: full schema lives in store's own SQLite file, no
shared-DB coupling with the bus. Implements every read method on
:class:`ArtifactBackend` plus the new ``create()`` write surface.

Phase 4a is the "writes work locally; reads from local-only" slice.
Phase 4b adds :class:`CompositeArtifactBackend` that unions
LocalArtifactStore with the bus-backed read fallback so the
existing bus-resident corpus remains visible while the new
write-path bakes in.

Response shapes mirror what :class:`BusBackedArtifactStore`
emits, so consumers (the viewer's prefetch path, the
``handle_artifact_*`` skill handlers) don't see a difference
when the backend swaps.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Optional


logger = logging.getLogger(__name__)

from store.artifacts import ArtifactBackend, ListResult


# Match the bus's caps so artifacts written via the local skill
# can be migrated to / from the bus side without surprise size
# rejections at the boundary.
DEFAULT_MAX_CHARS = 4000
HARD_MAX_CHARS = 20_000
MAX_ARTIFACT_BYTES = 10 * 1024 * 1024  # 10 MiB
MAX_LIST_LIMIT = 100
MAX_GREP_MATCHES = 100
MAX_GREP_CONTEXT_LINES = 50
MAX_HEAD_TAIL_LINES = 1000


_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifacts (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    title           TEXT NOT NULL,
    producer        TEXT NOT NULL DEFAULT '',
    session_id      TEXT NOT NULL DEFAULT '',
    trace_id        TEXT NOT NULL DEFAULT '',
    content_type    TEXT NOT NULL DEFAULT 'text/plain',
    size_bytes      INTEGER NOT NULL,
    sha256          TEXT NOT NULL,
    metadata        TEXT NOT NULL DEFAULT '{}',
    source_artifacts TEXT NOT NULL DEFAULT '[]',
    content         TEXT NOT NULL,
    ttl             TEXT,
    -- Sub-second precision so the canonical "newest first" ordering
    -- in list() resolves deterministically when two creates land in
    -- the same wall-second (a normal occurrence under test load and
    -- not unusual for an idle agent receiving a burst). The bus's
    -- schema uses second precision; we tighten here because the
    -- store-local DB is the new source of truth.
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifacts(session_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_kind ON artifacts(kind);
CREATE INDEX IF NOT EXISTS idx_artifacts_producer ON artifacts(producer);
CREATE INDEX IF NOT EXISTS idx_artifacts_created ON artifacts(created_at);
"""


@dataclass(frozen=True)
class _BoundedText:
    text: str
    truncated: bool
    start_line: Optional[int] = None
    end_line: Optional[int] = None


class LocalArtifactStore(ArtifactBackend):
    """SQLite-backed artifact store owned by the store agent.

    The connection is opened lazily on first use and closed via
    :meth:`close`. Writes are serialized through a per-instance
    threading lock — sqlite3 connections are not safe to share
    across threads under the default driver, and the store
    agent's HTTP viewer thread can race with the asyncio request
    handler unless we serialize.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        # ``check_same_thread=False`` lets the connection serve
        # both the asyncio loop and the viewer's HTTP thread; the
        # explicit lock below restores safety.
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

    # -- lifecycle --------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.executescript(_SCHEMA)
            conn.commit()
            self._conn = conn
        return self._conn

    async def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # -- ArtifactBackend writes ------------------------------------------

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
        """Insert a new artifact and return its metadata.

        Returns ``{"error": ...}`` on validation failures rather
        than raising — the skill handler passes the envelope
        straight through to the bus client, which expects a dict.
        """
        if not kind:
            return {"error": "kind is required"}
        if not title:
            return {"error": "title is required"}
        raw = content.encode("utf-8")
        if len(raw) > MAX_ARTIFACT_BYTES:
            return {
                "error": (
                    f"content exceeds maximum size of {MAX_ARTIFACT_BYTES} bytes"
                )
            }
        new_id = artifact_id or f"art_{uuid.uuid4().hex[:12]}"
        sha = hashlib.sha256(raw).hexdigest()
        try:
            meta_json = json.dumps(metadata or {}, sort_keys=True)
            sources_json = json.dumps(source_artifacts or [], sort_keys=True)
        except (TypeError, ValueError) as exc:
            # ``metadata`` is annotated as ``dict[str, Any]`` but
            # callers can supply non-JSON-serializable values
            # (datetime / bytes / set raise TypeError) or
            # circular references (raise ValueError). Catch both
            # here so the handler surfaces a clean error envelope
            # instead of letting either exception bubble up and
            # fail the skill call.
            logger.warning(
                "rejecting non-JSON-serializable metadata/source_artifacts: %s: %s",
                type(exc).__name__, exc,
            )
            return {
                "error": "metadata and source_artifacts must be JSON-serializable",
            }
        return await asyncio.to_thread(
            self._sync_create,
            new_id, kind, title, producer, session_id, trace_id,
            content_type, len(raw), sha, meta_json, sources_json, content, ttl,
        )

    def _sync_create(
        self,
        artifact_id: str, kind: str, title: str, producer: str,
        session_id: str, trace_id: str, content_type: str,
        size_bytes: int, sha: str, metadata_json: str,
        sources_json: str, content: str, ttl: Optional[str],
    ) -> dict[str, Any]:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO artifacts (
                        id, kind, title, producer, session_id, trace_id,
                        content_type, size_bytes, sha256, metadata,
                        source_artifacts, content, ttl
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        artifact_id, kind, title, producer, session_id, trace_id,
                        content_type, size_bytes, sha, metadata_json,
                        sources_json, content, ttl,
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                # Python's sqlite3 driver opens an implicit
                # transaction for the INSERT; if the statement
                # raises, the transaction is left "aborted" until
                # we explicitly roll back. Skipping the rollback
                # would leave subsequent writes failing with
                # "cannot operate on an aborted transaction" and
                # could keep the DB locked.
                conn.rollback()
                return {"error": f"duplicate artifact id: {exc}"}
            except sqlite3.Error as exc:
                # OperationalError (DB locked / disk I/O), DatabaseError
                # (corruption), and other sqlite3.Error subclasses
                # would otherwise propagate out of the skill
                # handler as exceptions; the read-side methods
                # all return ``{"error": ...}`` envelopes so the
                # write side honors the same contract. Log the
                # full exception under our logger for forensics
                # — the envelope keeps to a stable string so
                # internal-state details don't leak to bus
                # clients.
                conn.rollback()
                logger.warning(
                    "local store error during create: %s: %s",
                    type(exc).__name__, exc, exc_info=True,
                )
                return {"error": "local store error"}
        return self._sync_metadata(artifact_id) or {"error": "create succeeded but metadata read failed"}

    # -- ArtifactBackend reads -------------------------------------------

    @staticmethod
    def _store_error_envelope(op: str, exc: sqlite3.Error) -> dict[str, Any]:
        """Stable error envelope for sqlite3 failures during reads.

        Same shape and policy as the broader
        ``_sync_create``-side handler: log the full exception
        under our logger (``exc_info=True``) for forensics,
        return a sanitized ``{"error": "local store error"}``
        so internal-state details (table names, file paths)
        don't leak through to bus clients.
        """
        logger.warning(
            "local store error during %s: %s: %s",
            op, type(exc).__name__, exc, exc_info=True,
        )
        return {"error": "local store error"}

    async def metadata(self, artifact_id: str) -> dict[str, Any]:
        try:
            result = await asyncio.to_thread(self._sync_metadata, artifact_id)
        except sqlite3.Error as exc:
            return self._store_error_envelope("metadata", exc)
        return result or {"error": "artifact not found"}

    def _sync_metadata(self, artifact_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                """
                SELECT id, kind, title, producer, session_id, trace_id,
                       content_type, size_bytes, sha256, metadata,
                       source_artifacts, created_at, ttl
                FROM artifacts WHERE id = ?
                """,
                (artifact_id,),
            ).fetchone()
        return _row_to_dict(row) if row else None

    async def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ) -> ListResult:
        try:
            return await asyncio.to_thread(
                self._sync_list, session_id, kind, producer, limit
            )
        except sqlite3.Error as exc:
            return self._store_error_envelope("list", exc)

    def _sync_list(
        self, session_id: str, kind: str, producer: str, limit: int,
    ) -> list[dict[str, Any]]:
        # Clamp to ``[0, MAX_LIST_LIMIT]`` rather than
        # ``[1, MAX_LIST_LIMIT]``: a caller asking for ``limit=0``
        # is asking "is anything matching?" without paying for
        # the rows, and the right answer is ``[]`` — silently
        # bumping to 1 (the previous behavior) returns content
        # they didn't want.
        clamped = max(0, min(int(limit), MAX_LIST_LIMIT))
        if clamped == 0:
            return []
        clauses, params = [], []
        if session_id:
            clauses.append("session_id = ?")
            params.append(session_id)
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if producer:
            clauses.append("producer = ?")
            params.append(producer)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            conn = self._connect()
            # SQLite's ``rowid`` increments monotonically per
            # INSERT, so it's a stable insertion-order tie-breaker
            # when two rows share the same ``created_at`` (still
            # possible at millisecond precision under burst
            # writes). Random UUID-derived ``id`` doesn't
            # represent insertion order so it can't serve.
            rows = conn.execute(
                f"""
                SELECT id, kind, title, producer, session_id, trace_id,
                       content_type, size_bytes, sha256, metadata,
                       source_artifacts, created_at, ttl
                FROM artifacts {where}
                ORDER BY created_at DESC, rowid DESC
                LIMIT ?
                """,
                (*params, clamped),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    async def get(
        self, artifact_id: str, *, offset: int = 0, max_chars: int = DEFAULT_MAX_CHARS,
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._sync_get, artifact_id, offset, max_chars
            )
        except sqlite3.Error as exc:
            return self._store_error_envelope("get", exc)

    def _sync_get(self, artifact_id: str, offset: int, max_chars: int) -> dict[str, Any]:
        meta = self._sync_metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        try:
            content = self._sync_content(artifact_id)
        except KeyError:
            return {"error": "artifact not found"}
        offset = max(0, int(offset))
        max_chars = _clamp_max_chars(max_chars)
        text = content[offset:offset + max_chars]
        return _view(meta, _BoundedText(text=text, truncated=offset + max_chars < len(content)))

    async def head(
        self, artifact_id: str, *, lines: int = 80, max_chars: int = DEFAULT_MAX_CHARS,
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._sync_head, artifact_id, lines, max_chars
            )
        except sqlite3.Error as exc:
            return self._store_error_envelope("head", exc)

    def _sync_head(self, artifact_id: str, lines: int, max_chars: int) -> dict[str, Any]:
        meta = self._sync_metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        try:
            content_lines = self._sync_content(artifact_id).splitlines()
        except KeyError:
            return {"error": "artifact not found"}
        n = _clamp_lines(lines)
        selected = content_lines[:n]
        bounded = _bound_lines(selected, max_chars, truncated=len(content_lines) > n)
        return _view(meta, bounded)

    async def tail(
        self, artifact_id: str, *, lines: int = 80, max_chars: int = DEFAULT_MAX_CHARS,
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._sync_tail, artifact_id, lines, max_chars
            )
        except sqlite3.Error as exc:
            return self._store_error_envelope("tail", exc)

    def _sync_tail(self, artifact_id: str, lines: int, max_chars: int) -> dict[str, Any]:
        meta = self._sync_metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        try:
            content_lines = self._sync_content(artifact_id).splitlines()
        except KeyError:
            return {"error": "artifact not found"}
        n = _clamp_lines(lines)
        selected = content_lines[-n:] if n else []
        bounded = _bound_lines(selected, max_chars, truncated=len(content_lines) > n)
        # When ``selected`` is empty (lines=0 or empty artifact)
        # the previous code produced ``start_line > end_line`` —
        # ``start = len + 1`` while ``end = len``. Signal "no
        # content" via (0, 0) instead so the line-range
        # invariant holds across all return paths.
        if not selected:
            bounded = _BoundedText(bounded.text, bounded.truncated, 0, 0)
        else:
            start = len(content_lines) - len(selected) + 1
            bounded = _BoundedText(
                bounded.text, bounded.truncated, start, len(content_lines),
            )
        return _view(meta, bounded)

    async def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = DEFAULT_MAX_CHARS,
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._sync_grep, artifact_id, pattern,
                context_lines, max_matches, max_chars,
            )
        except sqlite3.Error as exc:
            return self._store_error_envelope("grep", exc)

    def _sync_grep(
        self, artifact_id: str, pattern: str,
        context_lines: int, max_matches: int, max_chars: int,
    ) -> dict[str, Any]:
        if not pattern:
            return {"error": "pattern is required"}
        try:
            regex = re.compile(pattern)
        except re.error as exc:
            return {"error": f"invalid regex pattern: {exc}"}
        meta = self._sync_metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        try:
            lines = self._sync_content(artifact_id).splitlines()
        except KeyError:
            return {"error": "artifact not found"}
        ctx = max(0, min(int(context_lines), MAX_GREP_CONTEXT_LINES))
        # ``max_matches=0`` is a legitimate "count, don't return"
        # query — useful when the caller only needs to know
        # whether matches exist. Clamp to ``[0, MAX_GREP_MATCHES]``
        # rather than ``[1, ...]`` so the response can carry
        # ``matches=N, returned_matches=0`` instead of forcing
        # at least one block back.
        cap = max(0, min(int(max_matches), MAX_GREP_MATCHES))

        blocks: list[str] = []
        matches = 0
        for idx, line in enumerate(lines):
            if not regex.search(line):
                continue
            matches += 1
            if len(blocks) < cap:
                start = max(0, idx - ctx)
                end = min(len(lines), idx + ctx + 1)
                blocks.append(
                    f"--- match {matches} lines {start + 1}-{end} ---\n"
                    + "\n".join(lines[start:end])
                )

        bounded = _bound_text(
            "\n\n".join(blocks), max_chars,
            truncated=matches > len(blocks),
        )
        return {
            "artifact": meta,
            "text": bounded.text,
            "truncated": bounded.truncated,
            "matches": matches,
            "returned_matches": len(blocks),
        }

    async def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = DEFAULT_MAX_CHARS,
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._sync_excerpt, artifact_id, start_line, end_line, max_chars,
            )
        except sqlite3.Error as exc:
            return self._store_error_envelope("excerpt", exc)

    def _sync_excerpt(
        self, artifact_id: str, start_line: int, end_line: int, max_chars: int,
    ) -> dict[str, Any]:
        meta = self._sync_metadata(artifact_id)
        if meta is None:
            return {"error": "artifact not found"}
        try:
            content_lines = self._sync_content(artifact_id).splitlines()
        except KeyError:
            return {"error": "artifact not found"}
        start_line = max(1, int(start_line))
        end_line = max(start_line, int(end_line))
        selected = content_lines[start_line - 1:end_line]
        bounded = _bound_lines(
            selected, max_chars,
            truncated=end_line < len(content_lines),
        )
        if not selected:
            # ``start_line`` past EOF (or the slice happens to be
            # empty for some other reason) — the previous code
            # would emit ``start_line=<requested>, end_line=<len>``
            # which violates start ≤ end. Signal "no content" via
            # (0, 0) consistent with the same pattern in tail().
            bounded = _BoundedText(bounded.text, bounded.truncated, 0, 0)
        else:
            bounded = _BoundedText(
                bounded.text, bounded.truncated,
                start_line, min(end_line, len(content_lines)),
            )
        return _view(meta, bounded)

    # -- internals --------------------------------------------------------

    def _sync_content(self, artifact_id: str) -> str:
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                "SELECT content FROM artifacts WHERE id = ?",
                (artifact_id,),
            ).fetchone()
        if row is None:
            raise KeyError(artifact_id)
        return str(row["content"])


# ---------------------------------------------------------------------------
# helpers (private — module-internal, not part of the ABC contract)
# ---------------------------------------------------------------------------


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    out: dict[str, Any] = dict(row)
    for json_field in ("metadata", "source_artifacts"):
        raw = out.get(json_field)
        if isinstance(raw, str):
            try:
                out[json_field] = json.loads(raw)
            except json.JSONDecodeError:
                # On corrupt JSON, fall back to the empty
                # shape-compatible value (``{}`` for metadata,
                # ``[]`` for source_artifacts) rather than crash
                # the whole list response — a single bad row
                # shouldn't take out the others. The corrupt
                # bytes are gone from the dict but the row is
                # still readable via :meth:`_sync_content` for
                # forensics.
                out[json_field] = {} if json_field == "metadata" else []
    return out


def _clamp_max_chars(max_chars: int) -> int:
    return max(1, min(int(max_chars), HARD_MAX_CHARS))


def _clamp_lines(lines: int) -> int:
    return max(0, min(int(lines), MAX_HEAD_TAIL_LINES))


def _bound_text(text: str, max_chars: int, *, truncated: bool) -> _BoundedText:
    cap = _clamp_max_chars(max_chars)
    if len(text) <= cap:
        return _BoundedText(text=text, truncated=truncated)
    return _BoundedText(text=text[:cap], truncated=True)


def _bound_lines(lines: list[str], max_chars: int, *, truncated: bool) -> _BoundedText:
    return _bound_text("\n".join(lines), max_chars, truncated=truncated)


def _view(meta: dict[str, Any], bounded: _BoundedText) -> dict[str, Any]:
    """Format a bounded read result the same way the bus does.

    Mirrors :func:`bus.artifacts.view_response`'s shape so the
    response is wire-compatible with what
    :class:`BusBackedArtifactStore` returns from the same call.
    """
    payload: dict[str, Any] = {
        "artifact": meta,
        "text": bounded.text,
        "truncated": bounded.truncated,
    }
    if bounded.start_line is not None:
        payload["start_line"] = bounded.start_line
    if bounded.end_line is not None:
        payload["end_line"] = bounded.end_line
    return payload
