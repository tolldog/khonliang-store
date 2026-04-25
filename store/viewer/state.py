"""In-memory session/tab state for the viewer.

Sessions own tabs; tabs hold an artifact reference (id + optional
view_hint) plus enough metadata for the server to render. State is
explicitly volatile — there's no durability surface (per the FR:
"Persistence: none of its own. Every render fetches from bus/store.").

Thread-safe: the HTTP server runs on a worker thread separate from
whatever creates sessions, so all mutations go through a lock.
"""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class ArtifactRef:
    """Stable handle to an artifact stored elsewhere (today: bus)."""

    id: str
    view_hint: str = ""


@dataclass
class Tab:
    """A single rendered artifact within a session.

    Carries the prefetched artifact payload directly. Per the FR's
    "render fetches from bus/store" line we did consider fetching
    on every browser GET; we pre-fetch instead because it (a)
    avoids reaching from the HTTP-server thread back into the
    agent's asyncio loop and (b) gives the user a stable view of
    the artifact at display() time. A future ``refresh`` skill can
    re-fetch and rebuild the tab if a live-view contract is needed.
    """

    tab_id: str
    artifact: ArtifactRef
    content_type: str
    body: bytes
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = 0.0


@dataclass
class Session:
    session_id: str
    layout: str = "tabs"
    tabs: dict[str, Tab] = field(default_factory=dict)
    created_at: float = 0.0


class SessionRegistry:
    """Thread-safe registry of viewer sessions.

    The registry is the only mutable state in the viewer process —
    keeping it small means restart-resilience is essentially free
    (sessions die with the process and that's the documented
    contract).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, Session] = {}

    def _now(self) -> float:
        return time.time()

    def create_session(self, *, layout: str = "tabs") -> Session:
        with self._lock:
            # 16 bytes = 128 bits of entropy. The session_id is the
            # only guard on viewer URLs (no auth on the HTTP surface
            # — see CLAUDE.md's local-trusted-env posture); 64 bits
            # was tight for a capability URL when the host port is
            # network-reachable. 128 bits is the modern floor.
            session_id = secrets.token_urlsafe(16)
            session = Session(
                session_id=session_id,
                layout=layout,
                created_at=self._now(),
            )
            self._sessions[session_id] = session
            return session

    def add_tab(
        self,
        session_id: str,
        artifact: ArtifactRef,
        *,
        content_type: str,
        body: bytes,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Tab:
        """Append an artifact as a new tab in ``session_id``.

        Raises :class:`KeyError` if the session is unknown — the
        caller should always supply a session id from the same
        ``display`` round-trip that created it.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise KeyError(f"unknown session: {session_id!r}")
            # 12 bytes = ~96 bits — tab_ids are scoped per session
            # so the threat model is laxer than session_id, but
            # bump to match the broader entropy posture.
            tab_id = secrets.token_urlsafe(12)
            tab = Tab(
                tab_id=tab_id,
                artifact=artifact,
                content_type=content_type,
                body=body,
                metadata=dict(metadata or {}),
                created_at=self._now(),
            )
            session.tabs[tab_id] = tab
            return tab

    def session_snapshot(
        self, session_id: str
    ) -> Optional[tuple["Session", tuple["Tab", ...]]]:
        """Return ``(session, tab_tuple)`` taken atomically.

        The HTTP server runs on a worker thread; per-tab DELETE
        requests can drop entries from another thread mid-render.
        Snapshotting under the lock avoids "dict changed size
        during iteration" and guarantees the renderer sees a
        single consistent set of tabs.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None
            return session, tuple(session.tabs.values())

    def get_tab(self, session_id: str, tab_id: str) -> Optional[Tab]:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None
            return session.tabs.get(tab_id)

    def drop_tab(self, session_id: str, tab_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False
            return session.tabs.pop(tab_id, None) is not None

    def drop_session(self, session_id: str) -> bool:
        with self._lock:
            return self._sessions.pop(session_id, None) is not None

    def snapshot(self) -> dict[str, Any]:
        """Compact debug view — caller-safe (returns plain dicts)."""
        with self._lock:
            return {
                "session_count": len(self._sessions),
                "tab_count": sum(len(s.tabs) for s in self._sessions.values()),
            }
