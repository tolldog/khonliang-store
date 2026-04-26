"""Composite artifact backend.

Layered backend that bridges Phase 4a's local-only writes with the
historical bus-resident corpus. Reads try ``local`` first, then
fall through to ``fallback`` (the bus-backed proxy) when the
local store reports "artifact not found"; writes go straight to
``local``. Phase 4c will retire ``fallback`` once the migration
has run cleanly.

The motivation: switching the Phase-4a default from ``bus`` to
``local`` would make every artifact created before the migration
invisible to the store. The composite layer keeps both
generations visible during the transition while writes
exclusively flow to the new home.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from store.artifacts import ArtifactBackend, ListResult
from store.local_store import MAX_LIST_LIMIT


logger = logging.getLogger(__name__)


# Sentinel produced by the read backends (both LocalArtifactStore
# and BusBackedArtifactStore) when an artifact isn't present.
# Centralizing the literal so the fall-through condition stays in
# step with what the underlying backends actually emit.
_NOT_FOUND = "artifact not found"


def _is_not_found(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("error") == _NOT_FOUND
    )


class CompositeArtifactBackend(ArtifactBackend):
    """Local-first reader, local-only writer.

    Writes are routed exclusively to ``local``; the fallback never
    sees a write. Read methods try ``local`` first; on a clean
    "artifact not found" envelope they fall through to ``fallback``.
    Other error envelopes from ``local`` (e.g.,
    ``"local store error"``) are treated as authoritative and
    returned directly — falling through on a transport error
    would mask a real local-side issue behind whatever the bus
    happened to know.
    """

    def __init__(
        self,
        local: ArtifactBackend,
        fallback: ArtifactBackend,
    ) -> None:
        self._local = local
        self._fallback = fallback

    @property
    def local(self) -> ArtifactBackend:
        """Read-only accessor for the local (write-target) half.

        Stable name so callers (the migration tooling in
        ``store.agent``, future Phase 4c work) don't have to
        reach into ``_local`` and tightly couple to private
        attributes.
        """
        return self._local

    @property
    def fallback(self) -> ArtifactBackend:
        """Read-only accessor for the fallback (read-source) half."""
        return self._fallback

    async def close(self) -> None:
        # Close both halves so neither leaks resources at agent
        # shutdown. Failures are logged and swallowed so a wedged
        # backend can't poison the other half's cleanup.
        for label, backend in (("local", self._local), ("fallback", self._fallback)):
            try:
                await backend.close()
            except Exception as exc:  # noqa: BLE001
                # ``exc_info=True`` to match the rest of the
                # repo's error-log convention — close-time
                # failures are rare and the traceback is the
                # main diagnostic when one happens.
                logger.warning(
                    "composite backend close failed for %s: %s: %s",
                    label, type(exc).__name__, exc,
                    exc_info=True,
                )

    # -- writes go local-only --------------------------------------------

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
        return await self._local.create(
            kind=kind,
            title=title,
            content=content,
            producer=producer,
            session_id=session_id,
            trace_id=trace_id,
            content_type=content_type,
            metadata=metadata,
            source_artifacts=source_artifacts,
            artifact_id=artifact_id,
            ttl=ttl,
        )

    # -- read fall-through ------------------------------------------------

    async def metadata(self, artifact_id: str) -> dict[str, Any]:
        result = await self._local.metadata(artifact_id)
        if _is_not_found(result):
            return await self._fallback.metadata(artifact_id)
        return result

    async def get(
        self, artifact_id: str, *, offset: int = 0, max_chars: int = 4000,
    ) -> dict[str, Any]:
        result = await self._local.get(
            artifact_id, offset=offset, max_chars=max_chars,
        )
        if _is_not_found(result):
            return await self._fallback.get(
                artifact_id, offset=offset, max_chars=max_chars,
            )
        return result

    async def head(
        self, artifact_id: str, *, lines: int = 80, max_chars: int = 4000,
    ) -> dict[str, Any]:
        result = await self._local.head(
            artifact_id, lines=lines, max_chars=max_chars,
        )
        if _is_not_found(result):
            return await self._fallback.head(
                artifact_id, lines=lines, max_chars=max_chars,
            )
        return result

    async def tail(
        self, artifact_id: str, *, lines: int = 80, max_chars: int = 4000,
    ) -> dict[str, Any]:
        result = await self._local.tail(
            artifact_id, lines=lines, max_chars=max_chars,
        )
        if _is_not_found(result):
            return await self._fallback.tail(
                artifact_id, lines=lines, max_chars=max_chars,
            )
        return result

    async def grep(
        self,
        artifact_id: str,
        *,
        pattern: str,
        context_lines: int = 10,
        max_matches: int = 10,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        result = await self._local.grep(
            artifact_id,
            pattern=pattern,
            context_lines=context_lines,
            max_matches=max_matches,
            max_chars=max_chars,
        )
        if _is_not_found(result):
            return await self._fallback.grep(
                artifact_id,
                pattern=pattern,
                context_lines=context_lines,
                max_matches=max_matches,
                max_chars=max_chars,
            )
        return result

    async def excerpt(
        self,
        artifact_id: str,
        *,
        start_line: int,
        end_line: int,
        max_chars: int = 4000,
    ) -> dict[str, Any]:
        result = await self._local.excerpt(
            artifact_id,
            start_line=start_line, end_line=end_line, max_chars=max_chars,
        )
        if _is_not_found(result):
            return await self._fallback.excerpt(
                artifact_id,
                start_line=start_line, end_line=end_line, max_chars=max_chars,
            )
        return result

    # -- list union ------------------------------------------------------

    async def list(
        self,
        *,
        session_id: str = "",
        kind: str = "",
        producer: str = "",
        limit: int = 20,
    ) -> ListResult:
        """Union of local + fallback rows, deduplicated by id.

        Local rows come first (they're authoritative — once
        migrated, the local copy is canonical). The result is
        clipped at ``limit`` so a large fallback corpus doesn't
        blow past the caller's budget.

        Error semantics differ by side:

        * Local error envelope → returned verbatim. A local-side
          failure is authoritative; substituting the fallback
          would mask a real local issue behind whatever the bus
          happened to know.
        * Fallback error envelope → degraded view. The local
          rows return as a best-effort list rather than masking
          the local data behind a fallback-side failure. Note
          that ``BusBackedArtifactStore`` only writes to its own
          logger on transport-level exceptions
          (``httpx.HTTPError``); 4xx/5xx and non-JSON responses
          surface as error envelopes without a log line, so a
          fallback failure here may be silent in the agent log.
        """
        # Clamp ``limit`` to ``[0, MAX_LIST_LIMIT]`` to mirror the
        # underlying backends' policy, and to avoid surprises:
        # negative ``limit`` would otherwise turn ``local[:limit]``
        # into an end-trimmed slice (Python list semantics) and
        # ``len(local) >= limit`` could short-circuit incorrectly.
        # ``limit=0`` collapses to an empty list immediately.
        limit = max(0, min(int(limit), MAX_LIST_LIMIT))
        if limit == 0:
            return []
        local = await self._local.list(
            session_id=session_id, kind=kind, producer=producer, limit=limit,
        )
        if isinstance(local, dict):
            return local
        if len(local) >= limit:
            # Local side already filled the budget; no need to
            # round-trip the fallback.
            return local[:limit]
        # Over-fetch the fallback by ``len(local)`` to compensate
        # for rows that will be discarded as duplicates of
        # local-side ids. Cap at ``MAX_LIST_LIMIT`` (shared with
        # local_store + the bus's REST clamp) so an oversized
        # request doesn't blow past either the caller's budget
        # or the bus's clamp. Worst-case the caller still sees
        # ``limit`` unique rows; without the over-fetch a
        # fully-overlapping fallback page could leave the merged
        # list under-filled even when more unique rows exist
        # further down.
        fallback_limit = min(MAX_LIST_LIMIT, limit + len(local))
        fallback = await self._fallback.list(
            session_id=session_id, kind=kind, producer=producer, limit=fallback_limit,
        )
        if isinstance(fallback, dict):
            # Fallback errored; surface what we got from local
            # rather than swallowing real data behind a transport
            # blip. The caller still sees a list — degraded view,
            # not failure.
            return local
        seen_ids = {
            item.get("id")
            for item in local
            if isinstance(item, dict) and item.get("id")
        }
        merged = list(local)
        for item in fallback:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            if not item_id:
                # Skip rows with a missing/empty id — they can't
                # participate in dedup and a caller that needs
                # them can hit the fallback directly.
                continue
            if item_id in seen_ids:
                continue
            # Update ``seen_ids`` as we go so a fallback page
            # that contains its own duplicates doesn't append
            # the same id twice.
            seen_ids.add(item_id)
            merged.append(item)
            if len(merged) >= limit:
                break
        return merged[:limit]
