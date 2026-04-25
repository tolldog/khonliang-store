"""Viewer subpackage for the store agent.

Public API:

* :class:`ArtifactRef` — value object for ``display`` callers.
* :class:`PreparedTab` — pre-fetched payload for one tab.
* :func:`display` — register a list of prepared tabs on a (lazy)
  viewer server and return ``{url, session_id, tab_ids}``.
* :func:`register_renderer` — extension hook for new content types.

Pre-fetching at the call site keeps the HTTP server thread free of
agent-loop callbacks; see ``server.py`` for the rationale.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from store.viewer.renderers import register_renderer  # noqa: F401 — re-export
from store.viewer.server import ensure_server
from store.viewer.state import ArtifactRef, Session

__all__ = [
    "ArtifactRef",
    "PreparedTab",
    "display",
    "register_renderer",
]


@dataclass(frozen=True)
class PreparedTab:
    """One artifact already fetched and ready to render."""

    artifact: ArtifactRef
    content_type: str
    body: bytes
    metadata: dict[str, Any] = field(default_factory=dict)


def display(
    prepared: Sequence[PreparedTab],
    *,
    layout: str = "tabs",
    host: Optional[str] = None,
    port: int = 0,
) -> dict[str, Any]:
    """Open (or reuse) the viewer and register prepared tabs.

    Returns ``{url, session_id, tab_ids}``. ``url`` opens the new
    session; ``tab_ids`` lets callers issue per-tab DELETE requests.

    ``layout='tabs'`` (default) shows one tab at a time;
    ``layout='split'`` is a hint the client uses to start in a
    split-pane state (the server stays dumb about layout).
    """
    if not prepared:
        raise ValueError("display() requires at least one PreparedTab")
    if layout not in {"tabs", "split"}:
        raise ValueError(f"unknown layout {layout!r}; expected 'tabs' or 'split'")

    server = ensure_server(host=host, port=port)
    session: Session = server.registry.create_session(layout=layout)
    tab_ids: list[str] = []
    for ptab in prepared:
        tab = server.registry.add_tab(
            session.session_id,
            ptab.artifact,
            content_type=ptab.content_type,
            body=ptab.body,
            metadata=ptab.metadata,
        )
        tab_ids.append(tab.tab_id)

    url = f"{server.base_url}/view/{session.session_id}"
    return {
        "url": url,
        "session_id": session.session_id,
        "tab_ids": tab_ids,
    }
