"""Store agent — Phase 1 scaffold + Phase 2 reads + Phase 3 viewer.

Phase 1 (``fr_store_4ea7d48b``) shipped the registered-but-empty
shell: subclass of :class:`BaseAgent`, ``agent_type = "store"``,
install/uninstall CLI matching the developer and reviewer pattern.
Phase 3 (``fr_store_d22556bb``) added the first real skill,
``display(artifacts)``, which lazily starts an HTTP viewer in a
worker thread and returns a URL the caller can open in a browser.
Phase 2 (``fr_store_08c1c6b2``) adds the artifact read surface:
``artifact_get``, ``artifact_list``, ``artifact_metadata``,
``artifact_head``, ``artifact_tail``, ``artifact_grep``,
``artifact_excerpt``, all routed through an
:class:`ArtifactBackend` that today proxies to the bus's REST
surface. Phase 4 will swap in a SQLite-backed local backend
without touching the skill surface or the viewer.

Current skill surface:
    - ``health_check`` — inherited from :class:`BaseAgent`.
    - ``display(artifacts, layout='tabs')`` — lazy-start viewer,
      register tabs, return ``{url, session_id, tab_ids}``.
    - ``artifact_get``, ``artifact_list``, ``artifact_metadata``,
      ``artifact_head``, ``artifact_tail``, ``artifact_grep``,
      ``artifact_excerpt`` — bus-backed reads, response shape
      mirrors what bus emits today.

Usage::

    # Install into the bus
    python -m store.agent install --id store-primary --bus http://localhost:8788 --config config.yaml

    # Start (normally done by the bus on boot)
    python -m store.agent --id store-primary --bus http://localhost:8788 --config config.yaml

    # Uninstall
    python -m store.agent uninstall --id store-primary --bus http://localhost:8788 --config config.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any, Optional, Tuple

import yaml
from khonliang_bus import BaseAgent, Skill, handler

from store.artifacts import ArtifactBackend, BusBackedArtifactStore
from store.composite import CompositeArtifactBackend
from store.local_store import LocalArtifactStore
from store.viewer import ArtifactRef, PreparedTab, display as viewer_display

logger = logging.getLogger(__name__)


# Cap on parallel artifact fetches per display() call. Tuned for
# "interactive viewer call against a single bus" rather than batch
# ingest — pick something low enough that a 50-tab display doesn't
# burst into 50 simultaneous bus requests, but high enough that
# latency-bound (rather than CPU-bound) fetches don't serialize.
_FETCH_CONCURRENCY = 8

# Per-tab content cap for the viewer fetch. Sized so the renderer
# sees the artifact body in full for any artifact under ~2 MiB —
# orders of magnitude above the ``max_chars=4000`` token-budget
# default the read skills use. A larger artifact will be truncated
# at this boundary; the alternative (no cap, or a streamed body)
# would let a single huge artifact dominate the viewer's process
# memory, and the renderer pipeline assumes bytes-in-memory anyway.
_VIEWER_FETCH_CAP_CHARS = 2_000_000


def _build_backend(*, config_path: str, bus_url: str) -> ArtifactBackend:
    """Construct the artifact backend from YAML config.

    Reads ``[artifacts] backend`` (one of ``bus``, ``local``,
    ``composite``) and — for the local + composite backends —
    ``[artifacts] db_path`` (relative paths resolve against the
    config file's directory). Defaults to the bus-backed
    read-only proxy so existing deployments keep working
    without a config change.

    The selection lives here rather than in ``__init__`` so tests
    can call :meth:`StoreAgent.set_backend` without touching disk
    and so the agent process surfaces a clear log line about
    which backend is in use at startup.
    """
    cfg = _read_artifacts_config(config_path)
    backend_value = _str_or_none(cfg, "backend", config_path)
    backend_kind = (backend_value or "bus").strip().lower()
    if backend_kind == "local":
        db_path_value = _str_or_none(cfg, "db_path", config_path)
        db_path = _resolve_db_path(db_path_value, config_path)
        logger.info("store backend: local (db_path=%s)", db_path)
        return LocalArtifactStore(db_path)
    if backend_kind == "composite":
        db_path_value = _str_or_none(cfg, "db_path", config_path)
        db_path = _resolve_db_path(db_path_value, config_path)
        logger.info(
            "store backend: composite (local db_path=%s, fallback bus_url=%s)",
            db_path, bus_url,
        )
        return CompositeArtifactBackend(
            local=LocalArtifactStore(db_path),
            fallback=BusBackedArtifactStore(bus_url),
        )
    if backend_kind != "bus":
        logger.warning(
            "unknown artifacts.backend=%r in %s — falling back to 'bus'",
            backend_kind, config_path,
        )
    logger.info("store backend: bus (bus_url=%s)", bus_url)
    return BusBackedArtifactStore(bus_url)


def _str_or_none(cfg: dict[str, Any], key: str, config_path: str) -> Optional[str]:
    """Return the YAML-decoded ``cfg[key]`` if it's a string, else None.

    YAML can decode ``backend: 1`` as an int and ``backend: ~`` as
    None; the previous code went straight to ``.strip().lower()``
    on the result and crashed at startup on either. Now we
    log-and-default on any non-string so the agent boots
    successfully and the operator sees the typo in the log.
    """
    value = cfg.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    logger.warning(
        "invalid artifacts.%s=%r in %s — expected string; using default",
        key, value, config_path,
    )
    return None


def _read_artifacts_config(config_path: str) -> dict[str, Any]:
    """Return the ``artifacts`` mapping from the YAML config, or {}.

    Missing file or missing ``artifacts`` section both yield an
    empty dict so the default backend kicks in. Config errors are
    logged at WARNING and treated as "use defaults" — the agent
    starting up successfully is more valuable than a hard crash on
    a typo.
    """
    if not config_path:
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(
            "could not read store config %s: %s — using defaults",
            config_path, exc,
        )
        return {}
    if not isinstance(data, dict):
        return {}
    artifacts = data.get("artifacts")
    return artifacts if isinstance(artifacts, dict) else {}


def _resolve_db_path(db_path: Optional[str], config_path: str) -> str:
    """Pick a sensible default if ``db_path`` is unset.

    Default: ``store_artifacts.db`` next to the config file (or
    cwd if no config). Relative paths in the config resolve
    against the config dir so the agent works the same when
    started from a different working directory.
    """
    if db_path:
        if os.path.isabs(db_path):
            return db_path
        base = os.path.dirname(os.path.abspath(config_path)) if config_path else os.getcwd()
        return os.path.join(base, db_path)
    base = os.path.dirname(os.path.abspath(config_path)) if config_path else os.getcwd()
    return os.path.join(base, "store_artifacts.db")


class StoreAgent(BaseAgent):
    """Bus-native store agent.

    Phase-1 scaffold (`fr_store_4ea7d48b`) plus Phase-2 reads
    (`fr_store_08c1c6b2`) plus Phase-3 viewer surface
    (`fr_store_d22556bb`). The read surface is backed by an
    :class:`ArtifactBackend`; today that's a thin HTTP wrapper
    around the bus REST routes, Phase 4 will swap in a local
    SQLite implementation that owns the data.
    """

    agent_id = "store-primary"
    agent_type = "store"
    module_name = "store.agent"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._backend: ArtifactBackend = _build_backend(
            config_path=self.config_path,
            bus_url=self.bus_url,
        )

    def set_backend(self, backend: ArtifactBackend) -> ArtifactBackend:
        """Override the backend (tests + future Phase-4 swap).

        Returns the previous backend so callers that own its
        lifecycle can close it themselves. We don't auto-close
        because (a) tests pass in fakes that have no close()
        anyway, and (b) doing it for the caller would force this
        method to be async and would tangle test setup (the test
        harness installs the backend before the event loop starts).
        """
        previous = self._backend
        self._backend = backend
        return previous

    async def shutdown(self) -> None:
        """Tear down: close the artifact backend before disconnecting.

        ``BaseAgent.shutdown`` closes the WebSocket and the
        internal httpx client; the artifact backend has its own
        httpx client (the one that talks to the bus REST surface)
        which we own and must close too — otherwise process exit
        emits "unclosed client" warnings. ``ArtifactBackend.close``
        is part of the ABC contract (default no-op for stateless
        backends), so we can call it directly without dynamic
        lookup or shape guessing.
        """
        try:
            await self._backend.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "store backend close raised %s: %s",
                type(exc).__name__, exc,
            )
        await super().shutdown()

    def register_skills(self) -> list[Skill]:
        return [
            Skill(
                "artifact_list",
                "List artifact metadata. Filters: session_id, kind, "
                "producer; capped by limit. Does not return content.",
                {
                    "session_id": {"type": "string", "default": ""},
                    "kind": {"type": "string", "default": ""},
                    "producer": {"type": "string", "default": ""},
                    "limit": {"type": "integer", "default": 20},
                },
                since="0.4.0",
            ),
            Skill(
                "artifact_metadata",
                "Return metadata for a single artifact: size, kind, "
                "producer, content_type, refs.",
                {"id": {"type": "string", "required": True}},
                since="0.4.0",
            ),
            Skill(
                "artifact_get",
                "Bounded character window from a text artifact. "
                "Returns metadata + the slice [offset, offset+max_chars).",
                {
                    "id": {"type": "string", "required": True},
                    "offset": {"type": "integer", "default": 0},
                    "max_chars": {"type": "integer", "default": 4000},
                },
                since="0.4.0",
            ),
            Skill(
                "artifact_head",
                "Bounded beginning of a text artifact (line-based "
                "with a character cap so a single huge line still "
                "respects max_chars).",
                {
                    "id": {"type": "string", "required": True},
                    "lines": {"type": "integer", "default": 80},
                    "max_chars": {"type": "integer", "default": 4000},
                },
                since="0.4.0",
            ),
            Skill(
                "artifact_tail",
                "Bounded end of a text artifact.",
                {
                    "id": {"type": "string", "required": True},
                    "lines": {"type": "integer", "default": 80},
                    "max_chars": {"type": "integer", "default": 4000},
                },
                since="0.4.0",
            ),
            Skill(
                "artifact_grep",
                "Bounded regex/substring excerpts from a text "
                "artifact. Returns the first max_matches hits with "
                "context_lines of surrounding context, capped by "
                "max_chars total.",
                {
                    "id": {"type": "string", "required": True},
                    "pattern": {"type": "string", "required": True},
                    "context_lines": {"type": "integer", "default": 10},
                    "max_matches": {"type": "integer", "default": 10},
                    "max_chars": {"type": "integer", "default": 4000},
                },
                since="0.4.0",
            ),
            Skill(
                "artifact_excerpt",
                "Bounded explicit line range from a text artifact "
                "(1-indexed, inclusive end_line).",
                {
                    "id": {"type": "string", "required": True},
                    "start_line": {"type": "integer", "required": True},
                    "end_line": {"type": "integer", "required": True},
                    "max_chars": {"type": "integer", "default": 4000},
                },
                since="0.4.0",
            ),
            Skill(
                "artifact_create",
                "Persist a new artifact in the store. Returns the "
                "new artifact's metadata (id, sha256, size_bytes, "
                "created_at, …) on success or {error: ...} on "
                "validation failure (missing kind/title, oversized "
                "content, duplicate id). Only works against a "
                "write-capable backend — the default bus-backed "
                "backend is read-only and rejects with a clear "
                "error envelope.",
                {
                    "kind": {"type": "string", "required": True},
                    "title": {"type": "string", "required": True},
                    "content": {"type": "string", "required": True},
                    "producer": {"type": "string", "default": ""},
                    "session_id": {"type": "string", "default": ""},
                    "trace_id": {"type": "string", "default": ""},
                    "content_type": {
                        "type": "string", "default": "text/plain",
                    },
                    "metadata": {"type": "object", "default": {}},
                    "source_artifacts": {
                        "type": "array", "default": [],
                    },
                    "id": {
                        "type": "string", "default": "",
                        "description": (
                            "Optional caller-supplied id; "
                            "auto-generated when empty."
                        ),
                    },
                    "ttl": {"type": "string", "default": ""},
                },
                since="0.5.0",
            ),
            Skill(
                "artifact_migrate_from_bus",
                "Copy artifacts from the bus's REST surface into "
                "the local SQLite store, preserving artifact ids "
                "so callers see the same id under both backends. "
                "Returns {copied, skipped, errors, scanned, "
                "dry_run}, plus an 'error' key if the bus list "
                "request itself fails. Idempotent: ids already "
                "present locally count as 'skipped'. Requires "
                "backend=composite (LocalArtifactStore for the "
                "write side + BusBackedArtifactStore for the "
                "read source). The bus list endpoint does not "
                "expose a paging cursor today, so this skill "
                "migrates at most 'limit' artifacts (capped at "
                "100) per call — for larger corpora, run with "
                "different filter combinations (session_id, "
                "kind, producer aren't accepted yet but are "
                "the planned extension surface). dry_run logs "
                "what would happen without writing.",
                {
                    "limit": {
                        "type": "integer", "default": 100,
                        "description": (
                            "Page size for the bus list call; "
                            "capped at 100 by the bus."
                        ),
                    },
                    "dry_run": {"type": "boolean", "default": False},
                },
                since="0.6.0",
            ),
            Skill(
                "display",
                "Open (or reuse) the in-process viewer and register "
                "the supplied artifacts as tabs. Returns the viewer URL "
                "plus the new session_id and tab_ids. Renders by "
                "content_type — markdown, JSON, graphviz, code, plain "
                "text — extensible by registering new renderers.",
                {
                    "artifacts": {
                        # The handler accepts either a JSON-encoded
                        # string or a structured list (objects of
                        # {id, view_hint?} or bare id strings).
                        # Advertised as 'string|list' so bus clients
                        # that pass structured args don't trip a
                        # client-side validator that only expected
                        # one or the other.
                        "type": "string|list",
                        "required": True,
                        "description": (
                            "List of artifact refs OR JSON-encoded "
                            "string of the same. Each entry is either "
                            "an artifact id string OR an object "
                            "{id, view_hint?}. Comma-separated bare "
                            "ids are also accepted as a fallback."
                        ),
                    },
                    "layout": {
                        "type": "string",
                        "default": "tabs",
                        "description": (
                            "Currently only 'tabs' is implemented; "
                            "'split' is reserved for a follow-up FR "
                            "and rejected with a clear error today."
                        ),
                    },
                },
                since="0.3.0",
            ),
        ]

    # -- artifact read skills ---------------------------------------------

    @handler("artifact_list")
    async def handle_artifact_list(self, args: dict[str, Any]) -> dict[str, Any]:
        try:
            limit = _int_arg(args, "limit", 20)
        except ValueError as exc:
            return {"error": str(exc)}
        items = await self._backend.list(
            session_id=str(args.get("session_id") or ""),
            kind=str(args.get("kind") or ""),
            producer=str(args.get("producer") or ""),
            limit=limit,
        )
        # Pass error envelopes through verbatim — wrapping them as
        # {"artifacts": {"error": ...}} would let a callsite that
        # only checks for the top-level "error" key happily render
        # an "outage" envelope as a (corrupted) artifact list.
        if isinstance(items, dict) and "error" in items:
            return items
        return {"artifacts": items}

    @handler("artifact_metadata")
    async def handle_artifact_metadata(self, args: dict[str, Any]) -> dict[str, Any]:
        artifact_id = _required_id(args)
        if not artifact_id:
            return {"error": "id is required"}
        return await self._backend.metadata(artifact_id)

    @handler("artifact_get")
    async def handle_artifact_get(self, args: dict[str, Any]) -> dict[str, Any]:
        artifact_id = _required_id(args)
        if not artifact_id:
            return {"error": "id is required"}
        try:
            offset = _int_arg(args, "offset", 0)
            max_chars = _int_arg(args, "max_chars", 4000)
        except ValueError as exc:
            return {"error": str(exc)}
        return await self._backend.get(
            artifact_id, offset=offset, max_chars=max_chars,
        )

    @handler("artifact_head")
    async def handle_artifact_head(self, args: dict[str, Any]) -> dict[str, Any]:
        artifact_id = _required_id(args)
        if not artifact_id:
            return {"error": "id is required"}
        try:
            lines = _int_arg(args, "lines", 80)
            max_chars = _int_arg(args, "max_chars", 4000)
        except ValueError as exc:
            return {"error": str(exc)}
        return await self._backend.head(
            artifact_id, lines=lines, max_chars=max_chars,
        )

    @handler("artifact_tail")
    async def handle_artifact_tail(self, args: dict[str, Any]) -> dict[str, Any]:
        artifact_id = _required_id(args)
        if not artifact_id:
            return {"error": "id is required"}
        try:
            lines = _int_arg(args, "lines", 80)
            max_chars = _int_arg(args, "max_chars", 4000)
        except ValueError as exc:
            return {"error": str(exc)}
        return await self._backend.tail(
            artifact_id, lines=lines, max_chars=max_chars,
        )

    @handler("artifact_grep")
    async def handle_artifact_grep(self, args: dict[str, Any]) -> dict[str, Any]:
        artifact_id = _required_id(args)
        if not artifact_id:
            return {"error": "id is required"}
        pattern = str(args.get("pattern") or "")
        if not pattern:
            return {"error": "pattern is required"}
        try:
            context_lines = _int_arg(args, "context_lines", 10)
            max_matches = _int_arg(args, "max_matches", 10)
            max_chars = _int_arg(args, "max_chars", 4000)
        except ValueError as exc:
            return {"error": str(exc)}
        return await self._backend.grep(
            artifact_id,
            pattern=pattern,
            context_lines=context_lines,
            max_matches=max_matches,
            max_chars=max_chars,
        )

    @handler("artifact_excerpt")
    async def handle_artifact_excerpt(self, args: dict[str, Any]) -> dict[str, Any]:
        artifact_id = _required_id(args)
        if not artifact_id:
            return {"error": "id is required"}
        try:
            start = _required_int(args, "start_line")
            end = _required_int(args, "end_line")
            max_chars = _int_arg(args, "max_chars", 4000)
        except ValueError as exc:
            return {"error": str(exc)}
        return await self._backend.excerpt(
            artifact_id, start_line=start, end_line=end, max_chars=max_chars,
        )

    # -- artifact write skill ---------------------------------------------

    @handler("artifact_create")
    async def handle_artifact_create(self, args: dict[str, Any]) -> dict[str, Any]:
        kind = str(args.get("kind") or "").strip()
        if not kind:
            return {"error": "kind is required"}
        title = str(args.get("title") or "").strip()
        if not title:
            return {"error": "title is required"}
        content = args.get("content")
        if not isinstance(content, str):
            return {"error": "content must be a string"}
        # ``key in args`` rather than ``args.get(...) or default`` so a
        # caller-supplied falsey-but-invalid value (e.g.
        # ``metadata: []`` or ``source_artifacts: 0``) still trips the
        # isinstance check below instead of being silently coerced
        # to the default and waved through.
        metadata = args["metadata"] if "metadata" in args else {}
        if not isinstance(metadata, dict):
            return {"error": "metadata must be an object"}
        sources = args["source_artifacts"] if "source_artifacts" in args else []
        if not isinstance(sources, list):
            return {"error": "source_artifacts must be an array"}
        # ``id`` overlaps with the bus's wire field name; "" → auto.
        artifact_id = str(args.get("id") or "").strip()
        ttl = str(args.get("ttl") or "").strip() or None
        try:
            return await self._backend.create(
                kind=kind,
                title=title,
                content=content,
                producer=str(args.get("producer") or ""),
                session_id=str(args.get("session_id") or ""),
                trace_id=str(args.get("trace_id") or ""),
                content_type=str(args.get("content_type") or "text/plain"),
                metadata=metadata,
                source_artifacts=[str(s) for s in sources],
                artifact_id=artifact_id,
                ttl=ttl,
            )
        except NotImplementedError as exc:
            return {"error": str(exc)}

    # -- artifact migration ----------------------------------------------

    @handler("artifact_migrate_from_bus")
    async def handle_artifact_migrate_from_bus(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        """Copy bus-resident artifacts into the local SQLite store.

        Reads the bus's ``/v1/artifacts`` paginated list via a
        :class:`BusBackedArtifactStore` (the existing bus REST
        surface), pulls each artifact's content with
        ``backend.get(id, max_chars=large)``, then calls
        ``LocalArtifactStore.create(artifact_id=...)`` so ids are
        preserved. Idempotent: a duplicate-id INSERT raises
        :class:`sqlite3.IntegrityError`, which the local backend
        translates into ``{error: "duplicate artifact id"}``;
        we count those as ``skipped``.

        Pulls metadata in pages of ``limit`` (default 100, the
        bus's hard cap) and uses the ``id`` of the oldest seen
        artifact as the next page's exclusive upper bound. The
        bus list endpoint doesn't accept a cursor today, so we
        rely on filtering ids we've already seen — slightly
        wasteful for very large corpora but correct and simple.

        Requires the agent to have a write-capable local
        backend; ``backend=bus`` would have nowhere to copy to.
        """
        try:
            limit = _int_arg(args, "limit", 100)
        except ValueError as exc:
            return {"error": str(exc)}
        # Cap at 100 to match the bus's MAX_LIST_LIMIT — asking
        # for more wastes a round trip. Floor at 1 so the loop
        # terminates.
        if limit < 1:
            limit = 1
        if limit > 100:
            limit = 100
        dry_run = bool(args.get("dry_run"))

        local_target, fallback_source = _migration_endpoints(self._backend)
        if local_target is None:
            return {
                "error": (
                    "artifact_migrate_from_bus requires a local backend; "
                    "current backend is read-only"
                ),
            }
        if fallback_source is None:
            # Operator wants to migrate but didn't wire a fallback —
            # nothing to copy from. Surface the misconfiguration
            # explicitly rather than silently reporting 0 copied.
            return {
                "error": (
                    "no bus fallback configured; set backend=composite "
                    "to enable migration"
                ),
            }

        copied = 0
        skipped = 0
        errors: list[dict[str, Any]] = []
        scanned = 0

        # The bus's list endpoint accepts a ``limit`` but no
        # cursor / before_id, so calling it twice with the same
        # filter args returns the same first ``limit`` rows. A
        # naive paging loop would spin forever (after the first
        # page every subsequent page is "all already seen") or
        # exit after one page either way — we just call once and
        # honestly limit the migration to the first ``limit``
        # rows. Operators with larger corpora re-run via filter
        # variants once the bus's list surface grows a cursor
        # (Phase 4c follow-up).
        page = await fallback_source.list(limit=limit)
        if isinstance(page, dict):
            return {
                "copied": copied, "skipped": skipped,
                "errors": errors, "scanned": scanned,
                "error": page.get("error", "bus list failed"),
            }
        for meta in page:
            if not isinstance(meta, dict):
                continue
            aid = str(meta.get("id") or "")
            scanned += 1
            outcome = await _migrate_one(
                aid=aid, meta=meta,
                source=fallback_source,
                target=local_target,
                dry_run=dry_run,
            )
            if outcome == "copied":
                copied += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                errors.append({"id": aid, "error": outcome})

        return {
            "copied": copied,
            "skipped": skipped,
            "errors": errors,
            "scanned": scanned,
            "dry_run": dry_run,
        }

    # -- display ----------------------------------------------------------

    @handler("display")
    async def handle_display(self, args: dict[str, Any]) -> dict[str, Any]:
        raw = args.get("artifacts")
        try:
            refs = _parse_artifact_refs(raw)
        except ValueError as exc:
            return {"error": str(exc)}
        if not refs:
            return {"error": "artifacts is required (non-empty list)"}

        layout = str(args.get("layout") or "tabs").strip().lower() or "tabs"
        if layout != "tabs":
            return {
                "error": (
                    f"layout={layout!r} not supported yet; only 'tabs' is "
                    "implemented (split-pane is a follow-up FR)"
                )
            }

        # Pre-fetch every artifact while we're still on the event
        # loop — keeps the HTTP server thread free of cross-loop
        # plumbing. Fetches run concurrently with a small
        # concurrency cap so a 50-artifact display doesn't pay
        # 50 * round_trip_time of latency, and the bus doesn't
        # take a thundering-herd burst either.
        sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def _fetch_one(ref: ArtifactRef) -> PreparedTab:
            async with sem:
                try:
                    content_type, body, metadata = await self._fetch_for_viewer(ref.id)
                except Exception as exc:  # noqa: BLE001
                    content_type = "text/plain"
                    body = (
                        f"Failed to fetch artifact {ref.id}:\n"
                        f"{type(exc).__name__}: {exc}"
                    ).encode("utf-8")
                    metadata = {"fetch_error": True}
            return PreparedTab(
                artifact=ref,
                content_type=content_type,
                body=body,
                metadata=metadata,
            )

        prepared = list(
            await asyncio.gather(*[_fetch_one(r) for r in refs])
        )

        # Server start (and the actual session register) is sync —
        # offload so a slow first-time bind doesn't stall the loop.
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: viewer_display(prepared, layout=layout),
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": f"viewer display failed: {exc}"}
        return result

    async def _fetch_for_viewer(
        self, artifact_id: str
    ) -> Tuple[str, bytes, dict[str, Any]]:
        """Resolve an artifact for the renderer via the backend.

        Goes through ``self._backend`` directly rather than
        round-tripping through the bus — the viewer runs inside
        the store agent process, so an in-process call is the
        correct path. Phase 4 swaps ``self._backend`` for a local
        SQLite implementation without touching this method.
        """
        # Cap the body at :data:`_VIEWER_FETCH_CAP_CHARS`. Sized so
        # any artifact under ~2 MiB renders in full; larger
        # artifacts are truncated at the cap (the renderer
        # pipeline holds bytes-in-memory, so unbounded fetches
        # would let a single huge artifact dominate the viewer's
        # process memory).
        payload = await self._backend.get(
            artifact_id,
            offset=0,
            max_chars=_VIEWER_FETCH_CAP_CHARS,
        )
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"backend.get returned non-dict: {type(payload).__name__}"
            )
        if "error" in payload:
            raise RuntimeError(payload["error"])
        body_text = payload.get("text") or payload.get("body") or ""
        body = (
            body_text.encode("utf-8")
            if isinstance(body_text, str)
            else bytes(body_text)
        )
        meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        content_type = (
            meta.get("content_type")
            or payload.get("content_type")
            or "text/plain"
        )
        return content_type, body, dict(meta)


def _migration_endpoints(
    backend: ArtifactBackend,
) -> Tuple[Optional[ArtifactBackend], Optional[ArtifactBackend]]:
    """Pick out (local_target, fallback_source) from the live backend.

    For ``backend=composite`` both halves are reachable directly;
    for ``backend=local`` we don't have a bus fallback in the
    backend object, so the caller must surface a misconfiguration
    error. ``backend=bus`` has no local target — also a
    misconfiguration for the migration skill.
    """
    if isinstance(backend, CompositeArtifactBackend):
        # Reach into the composite to get both halves; the
        # migration explicitly needs to bypass the composite's
        # local-first read policy because it's the side
        # *populating* the local store.
        return backend._local, backend._fallback
    if isinstance(backend, LocalArtifactStore):
        # Local-only — no fallback configured, so nothing to
        # migrate from. The handler returns a clear error so the
        # operator switches to backend=composite.
        return backend, None
    return None, None


# Cap for the per-artifact content fetch during migration. Sized
# to ``MAX_ARTIFACT_BYTES`` (10 MiB) on the bus side so a single
# call pulls the full body. Larger artifacts can't exist on the
# bus side, so any truncation here would indicate a bug.
_MIGRATION_FETCH_CAP_CHARS = 11_000_000


async def _migrate_one(
    *,
    aid: str,
    meta: dict[str, Any],
    source: ArtifactBackend,
    target: ArtifactBackend,
    dry_run: bool,
) -> str:
    """Copy a single artifact from source → target.

    Returns ``"copied"`` / ``"skipped"`` / ``"<error string>"``.
    """
    if not aid:
        return "missing id"
    body = await source.get(aid, offset=0, max_chars=_MIGRATION_FETCH_CAP_CHARS)
    if not isinstance(body, dict):
        # An ABC-conforming backend always returns a dict; a stub
        # or a custom backend that violates the contract would
        # otherwise crash at ``body.get(...)``. Surface as a
        # per-artifact error rather than letting it abort the
        # whole migration run.
        return f"fetch returned non-dict: {type(body).__name__}"
    if "error" in body:
        return f"fetch failed: {body['error']}"
    text = body.get("text") or body.get("body") or ""
    if not isinstance(text, str):
        return "non-string content"
    if dry_run:
        return "copied"
    result = await target.create(
        kind=str(meta.get("kind") or ""),
        title=str(meta.get("title") or ""),
        content=text,
        producer=str(meta.get("producer") or ""),
        session_id=str(meta.get("session_id") or ""),
        trace_id=str(meta.get("trace_id") or ""),
        content_type=str(meta.get("content_type") or "text/plain"),
        metadata=meta.get("metadata") if isinstance(meta.get("metadata"), dict) else {},
        source_artifacts=(
            meta.get("source_artifacts")
            if isinstance(meta.get("source_artifacts"), list)
            else []
        ),
        artifact_id=aid,
        ttl=meta.get("ttl"),
    )
    if isinstance(result, dict) and "error" in result:
        err = result["error"]
        if "duplicate artifact id" in err:
            return "skipped"
        return f"create failed: {err}"
    return "copied"


def _required_id(args: dict[str, Any]) -> str:
    """Pull and trim the ``id`` arg; empty / missing returns ''."""
    return str(args.get("id") or "").strip()


def _coerce_int(name: str, value: Any) -> int:
    """Coerce a non-empty value to int with the project's strict policy.

    Reject:

    * ``bool`` — ``True``/``False`` would silently become 1/0
      because ``bool`` is a subclass of ``int`` in Python.
    * Non-integer floats (``1.9`` would silently truncate to ``1``
      via ``int()`` and change the request — different from what
      the caller asked for).

    Accept integer-valued floats (``1.0`` → ``1``) since JSON
    can serialize ``1`` as either ``1`` or ``1.0`` depending on
    the encoder; treating them as equivalent is friendlier than
    rejecting a wire-format quirk. String-typed numerics
    (``"100"``) coerce normally.

    Raises ``ValueError`` with a per-arg message on any other
    shape; the caller wraps it in an ``{"error": ...}`` envelope.
    """
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{name} must be an integer")
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _int_arg(args: dict[str, Any], name: str, default: int) -> int:
    """Coerce ``args[name]`` to int.

    Missing / empty values fall back to ``default``. Provided
    values pass through :func:`_coerce_int` for the bool / non-
    integer-float / non-numeric rejection policy. Silently
    swallowing junk input would change request semantics (e.g.
    ``offset='abc'`` becoming ``offset=0``) and is exactly the
    kind of corruption that's hard to debug after the fact.
    """
    value = args.get(name)
    if value is None or value == "":
        return default
    return _coerce_int(name, value)


def _required_int(args: dict[str, Any], name: str) -> int:
    """Like :func:`_int_arg` but with no default — missing is an error.

    Used for required positional integers (``start_line`` /
    ``end_line``) where falling back to a default would change the
    request rather than fail loudly.
    """
    value = args.get(name)
    if value is None or value == "":
        raise ValueError(f"{name} is required")
    return _coerce_int(name, value)


def _parse_artifact_refs(raw: Any) -> list[ArtifactRef]:
    """Coerce the bus arg into a list of :class:`ArtifactRef`.

    Accepts:
    * a JSON list of strings (artifact ids)
    * a JSON list of objects ``{id, view_hint?}``
    * a comma-separated string of bare ids (fallback for shells)
    """
    if raw in (None, ""):
        return []
    items: Any = raw
    if isinstance(raw, str):
        try:
            items = json.loads(raw)
        except (TypeError, ValueError):
            # Fall back to comma-split.
            return [
                ArtifactRef(id=tok.strip())
                for tok in raw.split(",")
                if tok.strip()
            ]
    if not isinstance(items, list):
        raise ValueError(
            f"artifacts must be a JSON list (or comma-separated string), "
            f"got {type(items).__name__}"
        )
    refs: list[ArtifactRef] = []
    for entry in items:
        if isinstance(entry, str):
            ref_id = entry.strip()
            if ref_id:
                refs.append(ArtifactRef(id=ref_id))
        elif isinstance(entry, dict):
            ref_id = str(entry.get("id") or "").strip()
            if not ref_id:
                raise ValueError(f"artifact entry missing 'id': {entry!r}")
            hint = str(entry.get("view_hint") or "").strip()
            refs.append(ArtifactRef(id=ref_id, view_hint=hint))
        else:
            raise ValueError(
                f"artifact entry must be str or dict, got {type(entry).__name__}"
            )
    return refs


def main() -> None:
    from khonliang_bus import add_version_flag

    # Omit `prog` so argparse derives it from argv[0]. The package
    # installs a `khonliang-store` console script AND is runnable via
    # `python -m store.agent`; hard-coding one name makes the
    # --help output misleading when invoked via the other path.
    parser = argparse.ArgumentParser(
        description="khonliang-store bus agent",
    )
    add_version_flag(parser)
    parser.add_argument(
        "command",
        nargs="?",
        choices=["install", "uninstall"],
        help="install or uninstall from the bus",
    )
    parser.add_argument("--id", default="store-primary")
    parser.add_argument("--bus", default="http://localhost:8788")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if args.command in ("install", "uninstall"):
        StoreAgent.from_cli(
            [
                args.command,
                "--id", args.id,
                "--bus", args.bus,
                "--config", args.config,
            ]
        )
        return

    agent = StoreAgent(
        agent_id=args.id,
        bus_url=args.bus,
        config_path=args.config,
    )
    asyncio.run(agent.start())


if __name__ == "__main__":
    main()
