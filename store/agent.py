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
import sys
from typing import Any, Tuple

from khonliang_bus import BaseAgent, Skill, handler

from store.artifacts import ArtifactBackend, BusBackedArtifactStore
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
        self._backend: ArtifactBackend = BusBackedArtifactStore(self.bus_url)

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


def _required_id(args: dict[str, Any]) -> str:
    """Pull and trim the ``id`` arg; empty / missing returns ''."""
    return str(args.get("id") or "").strip()


def _int_arg(args: dict[str, Any], name: str, default: int) -> int:
    """Coerce ``args[name]`` to int.

    Missing / empty values fall back to ``default``. String-typed
    numerics ('100' from a JSON payload) are accepted. Anything
    else (provided but not coercible) raises ``ValueError`` so the
    caller can return an explicit ``{"error": ...}`` envelope —
    silently swallowing junk input would change request semantics
    (offset='abc' silently becomes offset=0) and is exactly the
    kind of corruption that's hard to debug after the fact.

    Booleans are rejected explicitly because ``bool`` is a subclass
    of ``int`` in Python: ``True → 1`` and ``False → 0`` would let
    a JSON-client typo (``max_chars: true``) silently change the
    request to ``max_chars=1`` instead of failing loudly.
    """
    value = args.get(name)
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _required_int(args: dict[str, Any], name: str) -> int:
    """Like :func:`_int_arg` but with no default — missing is an error.

    Used for required positional integers (``start_line`` /
    ``end_line``) where falling back to a default would change the
    request rather than fail loudly.
    """
    value = args.get(name)
    if value is None or value == "":
        raise ValueError(f"{name} is required")
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


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
