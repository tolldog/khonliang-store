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
from khonliang_bus import BaseAgent, Skill, Welcome, WelcomeEntryPoint, handler

from store.artifacts import ArtifactBackend, BusBackedArtifactStore
from store.composite import CompositeArtifactBackend
from store.local_store import HARD_MAX_CHARS, LocalArtifactStore, MAX_LIST_LIMIT
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

    # Cold-start orientation surface (fr_khonliang-bus-lib_6a82732c).
    WELCOME = Welcome(
        role="artifact backend + viewer authority",
        mission=(
            "Owns durable artifact storage — metadata, content, "
            "hash-immutable records — and a browser-rendered viewer for "
            "ad-hoc inspection. Migrating from the bus's legacy artifact "
            "REST surface to local SQLite via a composite read fallback. "
            "Future backends (S3, GCS, network FS) plug into the "
            "ArtifactBackend ABC without consumer changes."
        ),
        not_responsible_for=[
            "FR / spec / milestone state (developer)",
            "corpus + knowledge store + concept graph (researcher)",
            "code review (reviewer)",
        ],
        entry_points=[
            WelcomeEntryPoint(
                skill="artifact_create",
                when_to_use="persist any payload (large diff, test output, JSON, markdown) to local SQLite — returns a stable artifact_id",
            ),
            WelcomeEntryPoint(
                skill="artifact_get",
                when_to_use="fetch artifact content (clamped to HARD_MAX_CHARS=20000); returns truncated=true when the body exceeds the cap",
            ),
            WelcomeEntryPoint(
                skill="artifact_list",
                when_to_use="browse with filters (kind, producer, session_id, trace_id); newest-first ordering",
            ),
            WelcomeEntryPoint(
                skill="display",
                when_to_use="lazily start a browser-rendered tabbed viewer for one or more artifacts; returns a URL",
            ),
            WelcomeEntryPoint(
                skill="artifact_grep",
                when_to_use="bounded pattern excerpts with context_lines around each match",
            ),
        ],
    )

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
                "backend=composite with a LocalArtifactStore "
                "write target. The fallback (read source) is any "
                "ArtifactBackend that implements the ABC; "
                "production wires BusBackedArtifactStore. The "
                "bus list endpoint does not "
                "expose a paging cursor today, so this skill "
                "migrates at most 'limit' artifacts (capped at "
                "100) per call. Larger corpora are gated on the "
                "bus-side cursor work (Phase 4c follow-up). "
                "dry_run logs what would happen without writing.",
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
        translates into an error envelope of the form
        ``{"error": "duplicate artifact id: <sqlite message>"}``;
        ``_migrate_one`` recognizes the prefix and counts those
        responses as ``skipped``.

        Single-page migration. The bus's list endpoint takes no
        cursor, so this skill processes only the first ``limit``
        rows (capped at the bus's ``MAX_LIST_LIMIT=100``) per
        call. Real cursor support is a Phase 4c follow-up; until
        then operators with corpora larger than 100 artifacts
        re-run after each call's rows have been migrated and
        will appear in subsequent ``list()`` results from
        further down the queue (this assumes deletion / TTL
        eviction surfaces older rows; otherwise migration of
        the long tail is gated on the bus-side cursor work).

        Requires the agent to have a write-capable local
        backend AND a bus fallback — i.e. ``backend=composite``.
        """
        try:
            limit = _int_arg(args, "limit", 100)
            dry_run = _bool_arg(args, "dry_run", False)
        except ValueError as exc:
            return {"error": str(exc)}
        # Cap at MAX_LIST_LIMIT (the bus's REST clamp; the
        # local store enforces the same value) — asking for
        # more wastes a round trip. Allow 0 as an explicit
        # no-op (e.g. a connectivity / configuration smoke
        # test that returns the standard response shape with
        # zero counts).
        if limit < 0:
            limit = 0
        if limit > MAX_LIST_LIMIT:
            limit = MAX_LIST_LIMIT

        # Validate endpoints BEFORE the ``limit==0`` no-op
        # short-circuit so a misconfigured backend doesn't get a
        # false-positive "all good" response. ``limit=0`` is meant
        # to be a "config + plumbing wired correctly?" smoke test;
        # silently reporting success when the backend has nothing
        # to migrate to/from would defeat that.
        local_target, fallback_source = _migration_endpoints(self._backend)
        if local_target is None:
            # Maintain the standard response shape on
            # misconfiguration paths so callers can rely on
            # consistent keys regardless of whether the run
            # succeeded, partially failed, or never started.
            # Name the detected backend type so operators see
            # the exact mismatch — a composite wired with a
            # custom local half is functionally distinct from a
            # plain bus backend, and "read-only" alone hid that.
            backend_type = type(self._backend).__name__
            return {
                "copied": 0, "skipped": 0,
                "errors": [], "scanned": 0,
                "dry_run": dry_run,
                "error": (
                    "artifact_migrate_from_bus requires "
                    "backend=composite with a LocalArtifactStore "
                    f"local half; current backend is {backend_type}"
                ),
            }
        if fallback_source is None:
            # Operator wants to migrate but didn't wire a fallback —
            # nothing to copy from. Surface the misconfiguration
            # explicitly rather than silently reporting 0 copied.
            return {
                "copied": 0, "skipped": 0,
                "errors": [], "scanned": 0,
                "dry_run": dry_run,
                "error": (
                    "no bus fallback configured; set backend=composite "
                    "to enable migration"
                ),
            }

        # Endpoints validated; ``limit=0`` is now the "config
        # confirmed, no actual migration requested" success path.
        if limit == 0:
            return {
                "copied": 0, "skipped": 0,
                "errors": [], "scanned": 0,
                "dry_run": dry_run,
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
            # Include ``dry_run`` here too so callers see a
            # consistent response shape on every code path,
            # not just the success branch.
            return {
                "copied": copied, "skipped": skipped,
                "errors": errors, "scanned": scanned,
                "dry_run": dry_run,
                "error": page.get("error", "bus list failed"),
            }
        for meta in page:
            if not isinstance(meta, dict):
                continue
            raw_id = str(meta.get("id") or "")
            # Strip whitespace to match the normalization done in
            # ``_required_id`` / ``handle_artifact_create``;
            # otherwise a whitespace-suffixed id round-trips into
            # local SQLite with the suffix and never matches a
            # subsequent ``art_xyz`` read.
            aid = raw_id.strip()
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
                # Preserve the pre-strip id (or a sentinel) in
                # error reports so an operator can grep the bus
                # corpus for the offending row. Three cases:
                # truly missing → ``<missing>``; a
                # whitespace-only id (which strips to empty) →
                # ``<whitespace-only id: '...'>`` so the
                # original whitespace is visible; otherwise the
                # raw id passes through.
                if raw_id == "":
                    reported_id = "<missing>"
                elif raw_id.strip() == "":
                    reported_id = f"<whitespace-only id: {raw_id!r}>"
                else:
                    reported_id = raw_id
                errors.append({"id": reported_id, "error": outcome})

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

    Migration only works when the write target is a
    :class:`LocalArtifactStore`: ``_migrate_one``'s
    duplicate-detection path checks for ``LocalArtifactStore``'s
    specific ``"duplicate artifact id"`` envelope, and a
    custom local half wouldn't increment the ``skipped`` count
    correctly. The fallback (read source) can be any
    ``ArtifactBackend`` that satisfies the ABC; tests use stub
    backends and production wires ``BusBackedArtifactStore``.

    For ``backend=composite`` the public ``local`` / ``fallback``
    accessors expose both halves so the migration doesn't depend
    on private attribute names. ``backend=local`` has no
    fallback (caller surfaces a clear error). ``backend=bus``
    has no local target — also a misconfiguration for
    migration.
    """
    if isinstance(backend, CompositeArtifactBackend):
        local = backend.local
        fallback = backend.fallback
        if isinstance(local, LocalArtifactStore):
            return local, fallback
        # Miswired composite (custom local half): the
        # duplicate-id envelope shape isn't guaranteed, so
        # decline rather than report misleading skip counts.
        return None, None
    if isinstance(backend, LocalArtifactStore):
        # Local-only — no fallback configured, so nothing to
        # migrate from. The handler returns a clear error so the
        # operator switches to backend=composite.
        return backend, None
    return None, None


# Cap for the per-artifact content fetch during migration. Sourced
# from :data:`store.local_store.HARD_MAX_CHARS` (which mirrors the
# bus's REST clamp) so changing the cap in one place propagates
# here too. The previous 11M ceiling was effectively dead — bus
# clamped the request to 20K anyway — but would have permitted
# very large transfers against a non-bus backend that honored
# ``max_chars`` directly. A fetch of an artifact larger than this
# cap returns ``truncated=True`` and ``_migrate_one`` records it
# as a per-artifact error rather than writing partial content.
# Phase 4c may revisit by switching migration to a streaming
# endpoint.
_MIGRATION_FETCH_CAP_CHARS = HARD_MAX_CHARS


def _bool_arg(args: dict[str, Any], name: str, default: bool = False) -> bool:
    """Coerce ``args[name]`` to bool with strict policy.

    Real ``bool`` values pass through unchanged. Common
    case-insensitive string forms are accepted because some bus
    clients route args through JSON or YAML and the human-typed
    shape varies:

    * truthy: ``"true"``, ``"1"``, ``"yes"``
    * falsy: ``"false"``, ``"0"``, ``"no"``, ``""`` (empty string
      maps to the default-falsy convention used elsewhere in
      this module)

    Anything else raises ``ValueError`` so the handler can
    return a clean envelope — silently treating ``"false"`` as
    truthy (the default ``bool(value)`` policy on non-empty
    strings) was the surprising behavior the previous version
    exhibited.
    """
    if name not in args:
        return default
    value = args[name]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in ("true", "1", "yes"):
            return True
        if norm in ("false", "0", "no", ""):
            return False
    raise ValueError(f"{name} must be a boolean")


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
    Pre-flight validates required fields and skips ids already
    present locally so dry-run counts match what a real run
    would do.
    """
    if not aid:
        return "missing id"

    # Match ``handle_artifact_create``'s normalization so a
    # whitespace-only ``"   "`` from the bus side doesn't pass
    # validation and end up persisted locally.
    kind = str(meta.get("kind") or "").strip()
    title = str(meta.get("title") or "").strip()
    if not kind:
        return "create failed: kind is required"
    if not title:
        return "create failed: title is required"

    # Existence check up front: a duplicate-id row will be
    # skipped on the real run, so report it in dry-run too. The
    # metadata round-trip is cheap (no content) and keeps
    # ``copied`` / ``skipped`` aligned across modes. Only the
    # explicit ``"artifact not found"`` envelope falls through
    # to fetch — a different error (e.g. ``"local store error"``
    # from a sqlite failure) surfaces per-artifact so a real
    # local-side issue isn't masked by an attempted create.
    existing = await target.metadata(aid)
    if isinstance(existing, dict):
        if "error" not in existing:
            return "skipped"
        if existing.get("error") != "artifact not found":
            return f"metadata failed: {existing['error']}"

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
    if body.get("truncated") is True:
        # Don't silently migrate partial content. The bus's REST
        # surface caps reads at HARD_MAX_CHARS=20000 today; this
        # cap could be raised in the future, but until then the
        # migration's content fetch must NOT be truncated. Surface
        # the truncation as a per-artifact error so the operator
        # knows which ids need a different transport.
        return "fetch truncated"
    text = body.get("text") or body.get("body") or ""
    if not isinstance(text, str):
        return "non-string content"
    if dry_run:
        return "copied"
    raw_sources = meta.get("source_artifacts")
    sources = (
        # Coerce every element to ``str`` to match
        # ``handle_artifact_create``'s normalization (avoids
        # persisting unexpected element types like ints).
        [str(s) for s in raw_sources]
        if isinstance(raw_sources, list)
        else []
    )
    raw_ttl = meta.get("ttl")
    # Match ``handle_artifact_create``'s normalization exactly:
    # ``str(raw or "").strip() or None``. Anything falsey
    # (including ``0`` / ``False`` / ``""`` / whitespace-only)
    # collapses to ``None`` so we don't persist invalid TTLs
    # like the literal string ``"0"`` or ``"False"`` that the
    # type system would read as "has TTL".
    ttl: Optional[str] = str(raw_ttl or "").strip() or None
    try:
        result = await target.create(
            kind=kind,
            title=title,
            content=text,
            producer=str(meta.get("producer") or ""),
            session_id=str(meta.get("session_id") or ""),
            trace_id=str(meta.get("trace_id") or ""),
            content_type=str(meta.get("content_type") or "text/plain"),
            metadata=meta.get("metadata") if isinstance(meta.get("metadata"), dict) else {},
            source_artifacts=sources,
            artifact_id=aid,
            ttl=ttl,
        )
    except NotImplementedError as exc:
        # ABC default raise (read-only backend) — should be
        # impossible from the composite path that selected
        # ``backend.local`` as the target, but a custom or
        # miswired backend could still hit this. Surface the
        # crash as a per-artifact error so the migration keeps
        # iterating instead of taking down the whole skill.
        return f"create failed: {exc}"
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
