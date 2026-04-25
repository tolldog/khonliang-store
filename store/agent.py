"""Store agent — Phase 1 scaffold + Phase 3 viewer skill.

Phase 1 (``fr_store_4ea7d48b``) shipped the registered-but-empty
shell: subclass of :class:`BaseAgent`, ``agent_type = "store"``,
install/uninstall CLI matching the developer and reviewer pattern.
Phase 3 (``fr_store_d22556bb``) adds the first real skill,
``display(artifacts)``, which lazily starts an HTTP viewer in a
worker thread and returns a URL the caller can open in a browser.

Phase 2 (artifact read skills) is intentionally still pending — the
viewer reads artifact bytes via the bus today (see ``_fetch_via_bus``)
so the user-facing surface ships before the read-skill migration.

Current skill surface:
    - ``health_check`` — inherited from :class:`BaseAgent`.
    - ``display(artifacts, layout='tabs')`` — lazy-start viewer,
      register tabs, return ``{url, session_id, tab_ids}``.

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
from typing import Any, Tuple

from khonliang_bus import BaseAgent, Skill, handler

from store.viewer import ArtifactRef, PreparedTab, display as viewer_display


class StoreAgent(BaseAgent):
    """Bus-native store agent.

    Phase-1 scaffold (`fr_store_4ea7d48b`) plus Phase-3 viewer
    surface (`fr_store_d22556bb`). Artifact read/write skills land
    in subsequent phases; today the viewer fetches artifact bytes
    from the bus.
    """

    agent_id = "store-primary"
    agent_type = "store"
    module_name = "store.agent"

    def register_skills(self) -> list[Skill]:
        return [
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
                        "description": "'tabs' (default) or 'split'",
                    },
                },
                since="0.3.0",
            ),
        ]

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
        if layout not in {"tabs", "split"}:
            return {"error": f"layout must be 'tabs' or 'split', got {layout!r}"}

        # Pre-fetch every artifact while we're still on the event
        # loop — keeps the HTTP server thread free of cross-loop
        # plumbing. A fetch failure for one ref becomes an inline
        # error tab so the rest of the session still renders.
        prepared: list[PreparedTab] = []
        for ref in refs:
            try:
                content_type, body, metadata = await self._fetch_via_bus(ref.id)
            except Exception as exc:  # noqa: BLE001
                content_type = "text/plain"
                body = (
                    f"Failed to fetch artifact {ref.id}:\n"
                    f"{type(exc).__name__}: {exc}"
                ).encode("utf-8")
                metadata = {"fetch_error": True}
            prepared.append(
                PreparedTab(
                    artifact=ref,
                    content_type=content_type,
                    body=body,
                    metadata=metadata,
                )
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

    async def _fetch_via_bus(
        self, artifact_id: str
    ) -> Tuple[str, bytes, dict[str, Any]]:
        """Resolve an artifact for the renderer via the bus.

        Phase 4 will swap this to a local read against the store's
        own backend once write-ownership migrates. The renderer /
        server / state modules don't see the read path either way.
        """
        result = await self.request(
            agent_type="bus",
            operation="artifact_get",
            args={"id": artifact_id},
        )
        payload = (result and result.get("result")) or {}
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"bus_artifact_get returned non-dict: {type(payload).__name__}"
            )
        body_text = payload.get("text") or payload.get("body") or ""
        body = body_text.encode("utf-8") if isinstance(body_text, str) else bytes(body_text)
        meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        content_type = (
            meta.get("content_type")
            or payload.get("content_type")
            or "text/plain"
        )
        return content_type, body, dict(meta)


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
