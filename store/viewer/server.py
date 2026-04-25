"""HTTP server backing the display(artifacts) skill.

Threaded :mod:`http.server` based — keeps the runtime stdlib-only
and avoids dragging an async framework into the agent for what's
fundamentally a single-process read-only render tier.

Lifecycle (per the FR):

* Lazy start on first ``ensure_server`` call. One server per agent
  process; subsequent calls return the cached instance.
* No idle-shutdown / no TTL. The viewer survives until the agent
  process exits.
* Bind on hostname (not localhost) so a browser on a different
  machine can reach the URL the skill returns.

Tabs carry their pre-fetched payload (``content_type / body /
metadata``); the server only renders, never fetches. Pre-fetching
runs on the agent's event loop at ``display`` time and avoids the
async-loop-from-thread cross-talk that would come with on-demand
fetching from inside the HTTP handler.
"""

from __future__ import annotations

import logging
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import urlparse

from store.viewer.renderers import render
from store.viewer.state import SessionRegistry
from store.viewer.templates import render_session_page

logger = logging.getLogger(__name__)


class ViewerServer:
    """Thread-owned HTTP server holding viewer state.

    A single instance per agent process is the documented contract;
    use :func:`ensure_server` rather than constructing directly.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        registry: SessionRegistry,
        public_host: Optional[str] = None,
    ) -> None:
        self.registry = registry
        handler_cls = _make_handler(self)
        self._server = ThreadingHTTPServer((host, port), handler_cls)
        # Bind address (what the socket actually listens on) is
        # separate from the externally-visible host (what we put in
        # the returned URL). Binding on the FQDN can fail with
        # EADDRNOTAVAIL on hosts where the DNS name doesn't resolve
        # to a locally-configured interface; bind 0.0.0.0 by default
        # and only use the public_host for URL composition.
        self.host = host
        self.public_host = public_host or host
        # Read the bound port back in case 0 was passed.
        self.port = self._server.server_address[1]
        self._thread: Optional[threading.Thread] = None

    @property
    def base_url(self) -> str:
        return f"http://{self.public_host}:{self.port}"

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="store-viewer-http",
            daemon=True,
        )
        self._thread.start()
        logger.info("viewer started at %s", self.base_url)

    def shutdown(self) -> None:
        try:
            self._server.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._server.server_close()
        except Exception:  # noqa: BLE001
            pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None


# Module-level singleton management
# ---------------------------------------------------------------------------


_LOCK = threading.Lock()
_SERVER: Optional[ViewerServer] = None


def ensure_server(
    *,
    host: Optional[str] = None,
    port: int = 0,
    registry: Optional[SessionRegistry] = None,
) -> ViewerServer:
    """Return the running viewer, starting it on first call.

    Subsequent calls return the cached instance — the registry
    passed on the first call wins, on the assumption that a single
    agent process owns the viewer for its lifetime.

    ``host`` is the bind address. The default (``None``) binds
    ``0.0.0.0`` so any interface can serve the viewer; the URL the
    caller gets back uses the externally-resolvable hostname (FQDN
    when available) so a browser on a different machine can reach
    it. Tests pass ``host="127.0.0.1"`` to scope the listener to
    loopback.
    """
    global _SERVER
    with _LOCK:
        if _SERVER is not None:
            return _SERVER
        if host is None:
            bind_host = "0.0.0.0"
            public_host = _resolve_external_host()
        else:
            bind_host = host
            # Caller-supplied host (typically a test using
            # 127.0.0.1) is also the public host.
            public_host = host
        srv = ViewerServer(
            host=bind_host,
            port=port,
            registry=registry or SessionRegistry(),
            public_host=public_host,
        )
        srv.start()
        _SERVER = srv
        return srv


def _reset_for_tests() -> None:
    """Test-only hook: stop and clear the cached singleton."""
    global _SERVER
    with _LOCK:
        if _SERVER is not None:
            _SERVER.shutdown()
            _SERVER = None


def _resolve_external_host() -> str:
    """Pick a hostname the URL we hand back can actually be hit on.

    Prefers the FQDN; falls back to the gethostname output. Doesn't
    return ``localhost`` — per the FR, the user runs the bus on a
    headless box and views from a laptop, so binding on loopback
    would defeat the purpose.
    """
    try:
        fqdn = socket.getfqdn()
    except OSError:
        fqdn = ""
    if fqdn and fqdn != "localhost.localdomain":
        return fqdn
    return socket.gethostname() or "0.0.0.0"


# Request handler factory
# ---------------------------------------------------------------------------


def _make_handler(server: ViewerServer) -> type[BaseHTTPRequestHandler]:
    """Build a handler class bound to ``server``.

    Done as a factory because :class:`BaseHTTPRequestHandler` is
    instantiated per-request by the framework — we need a stable
    place to hang the registry reference without using globals.
    """

    registry = server.registry

    class _Handler(BaseHTTPRequestHandler):
        # Quieter default logging — write through our logger, not
        # straight to stderr.
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            logger.debug("viewer http: " + fmt, *args)

        def do_GET(self) -> None:  # noqa: N802 — http.server convention
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) == 2 and parts[0] == "view":
                self._serve_session(parts[1])
                return
            if len(parts) == 1 and parts[0] == "healthz":
                self._send(200, "text/plain; charset=utf-8", b"ok")
                return
            self._send(404, "text/plain; charset=utf-8", b"not found")

        def do_DELETE(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            # DELETE /view/<session>/tab/<tab_id>
            if (
                len(parts) == 4
                and parts[0] == "view"
                and parts[2] == "tab"
            ):
                ok = registry.drop_tab(parts[1], parts[3])
                self._send(
                    204 if ok else 404,
                    "text/plain; charset=utf-8",
                    b"" if ok else b"not found",
                )
                return
            # DELETE /view/<session>
            if len(parts) == 2 and parts[0] == "view":
                ok = registry.drop_session(parts[1])
                self._send(
                    204 if ok else 404,
                    "text/plain; charset=utf-8",
                    b"" if ok else b"not found",
                )
                return
            self._send(404, "text/plain; charset=utf-8", b"not found")

        # -- helpers ---------------------------------------------------------

        def _serve_session(self, session_id: str) -> None:
            # Snapshot under the registry lock so a concurrent
            # DELETE doesn't mutate the tab list mid-render.
            snap = registry.session_snapshot(session_id)
            if snap is None:
                self._send(404, "text/plain; charset=utf-8", b"unknown session")
                return
            session, tabs = snap
            rendered: dict[str, str] = {}
            for tab in tabs:
                rendered[tab.tab_id] = render(
                    tab.content_type, tab.body, tab.metadata
                )
            html_body = render_session_page(
                session.session_id,
                layout=session.layout,
                tabs=tabs,
                rendered_panes=rendered,
            )
            self._send(200, "text/html; charset=utf-8", html_body.encode("utf-8"))

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

    return _Handler
