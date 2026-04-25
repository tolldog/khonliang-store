"""Extensible renderer registry for the viewer.

Every artifact rendered in a tab passes through ``render(content_type,
body, metadata)``. The content_type's parameters are stripped before
lookup (``application/json; charset=utf-8`` → ``application/json``)
so registrations don't have to enumerate charset variants. If no
registered renderer matches the base type, the raw ``<pre>`` fallback
is used.

Adding a new file type is the explicit extension hook — register a
renderer and the viewer will pick it up::

    from store.viewer.renderers import register_renderer

    @register_renderer("text/x-yaml")
    def _yaml(body, meta):
        ...
        return html

This is the "places for file type support" lever the FR design
calls out: future tabs can render new MIME types without touching
the rest of the viewer plumbing.
"""

from __future__ import annotations

import base64
import html
import json
import logging
import shutil
import subprocess
import threading
from typing import Any, Callable

logger = logging.getLogger(__name__)

# (body, metadata) → HTML fragment dropped into the tab pane.
RendererFn = Callable[[bytes, dict[str, Any]], str]

_REGISTRY: dict[str, RendererFn] = {}
# Guard registry mutations so a renderer registered after the HTTP
# server is up — uncommon but legal per the FR's extension-hook
# contract — can't race a concurrent ``render()`` on a worker
# thread. Reads under the lock keep the safety contract explicit
# rather than implicit-on-CPython-GIL.
_REGISTRY_LOCK = threading.RLock()


def _registry_snapshot() -> dict[str, RendererFn]:
    """Test-only hook: return a shallow copy of the renderer registry.

    Pair with :func:`_registry_restore` in a fixture so a test that
    registers a custom MIME type doesn't pollute later tests.
    """
    with _REGISTRY_LOCK:
        return dict(_REGISTRY)


def _registry_restore(snapshot: dict[str, RendererFn]) -> None:
    """Test-only hook: replace the registry contents with ``snapshot``."""
    with _REGISTRY_LOCK:
        _REGISTRY.clear()
        _REGISTRY.update(snapshot)


def register_renderer(content_type: str) -> Callable[[RendererFn], RendererFn]:
    """Register a renderer for ``content_type`` (exact match).

    Returns the original function so usage as a decorator stays
    natural::

        @register_renderer("application/json")
        def _json(body, meta): ...
    """
    ct = content_type.strip().lower()

    def deco(fn: RendererFn) -> RendererFn:
        with _REGISTRY_LOCK:
            _REGISTRY[ct] = fn
        return fn

    return deco


def render(content_type: str, body: bytes, metadata: dict[str, Any]) -> str:
    """Dispatch ``(body, metadata)`` to the registered renderer.

    Falls back to the raw ``<pre>`` renderer when no renderer is
    registered. Renderer errors are caught and rendered as a small
    error block — a single bad artifact must not 500 the viewer.
    """
    base = (content_type or "").split(";", 1)[0].strip().lower()
    with _REGISTRY_LOCK:
        fn = _REGISTRY.get(base) or _REGISTRY.get("text/plain") or _render_raw
    try:
        return fn(body, metadata)
    except Exception as exc:  # noqa: BLE001
        logger.warning("renderer failed for %s: %s", base or "<unknown>", exc)
        return (
            "<div class=\"render-error\">"
            f"<strong>Renderer failed:</strong> {html.escape(type(exc).__name__)}: "
            f"{html.escape(_short(exc))}"
            "</div>"
        )


def _short(exc: BaseException, *, limit: int = 200) -> str:
    body = str(exc).strip().splitlines()
    head = body[0] if body else ""
    if len(head) > limit:
        head = head[: limit - 1].rstrip() + "…"
    return head


def _decode(body: bytes) -> str:
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return body.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Built-in renderers
# ---------------------------------------------------------------------------


@register_renderer("text/plain")
def _render_raw(body: bytes, metadata: dict[str, Any]) -> str:
    """Default fallback: escaped <pre>. Kept generic so unknown
    content_types route here automatically (see :func:`render`).
    """
    return f"<pre class=\"raw\">{html.escape(_decode(body))}</pre>"


@register_renderer("application/json")
def _render_json(body: bytes, metadata: dict[str, Any]) -> str:
    """Pretty-print JSON inside ``<pre data-tree>``.

    The client-side script in :mod:`store.viewer.templates` decorates
    ``data-tree`` elements with a click-to-collapse affordance so
    deep payloads don't dominate the viewport. Richer tree rendering
    (key-by-key expand, search) is a follow-up if the simple
    affordance proves insufficient — the server's job is just the
    indent-2 stringify.
    """
    text = _decode(body)
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        # Not actually JSON — fall through to raw rendering rather
        # than failing. Common when callers tag bytes as JSON
        # optimistically.
        return _render_raw(body, metadata)
    pretty = json.dumps(parsed, indent=2, ensure_ascii=False)
    return (
        f"<pre data-tree class=\"json\">{html.escape(pretty)}</pre>"
    )


@register_renderer("text/markdown")
def _render_markdown(body: bytes, metadata: dict[str, Any]) -> str:
    """Hand the markdown source to the client-side ``marked`` CDN
    script. Server-side rendering would need a markdown library we
    don't currently depend on; deferring to the browser keeps the
    viewer stdlib-only.

    The wrapper carries ``data-markdown`` so the client script can
    find unrendered blocks regardless of where they appear.
    """
    text = _decode(body)
    return (
        "<div class=\"markdown\" data-markdown>"
        f"<pre data-source>{html.escape(text)}</pre>"
        "</div>"
    )


@register_renderer("text/vnd.graphviz")
def _render_graphviz(body: bytes, metadata: dict[str, Any]) -> str:
    """Render a graphviz source via ``dot -Tsvg``. Falls back to the
    raw source in a <pre> if ``dot`` isn't on PATH.

    SVG output is embedded as a base64 ``data:`` URL on an
    ``<img>`` tag. Browsers do not execute scripts inside an SVG
    that's loaded via ``<img src>`` (only inline ``<svg>`` or
    ``<iframe>``-loaded SVGs run scripts), so a malicious DOT
    source can't escalate into the viewer's origin via the
    rendered SVG. The single round-trip property is preserved —
    the SVG bytes ride inline in the page response.
    """
    if shutil.which("dot") is None:
        return (
            "<div class=\"graphviz-fallback\">"
            "<p><em>graphviz `dot` not found on PATH — showing source.</em></p>"
            f"{_render_raw(body, metadata)}"
            "</div>"
        )
    proc = subprocess.run(
        ["dot", "-Tsvg"],
        input=body,
        capture_output=True,
        timeout=10,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        return (
            "<div class=\"graphviz-fallback\">"
            f"<p><em>dot exited {proc.returncode}: {html.escape(err[:200])}</em></p>"
            f"{_render_raw(body, metadata)}"
            "</div>"
        )
    encoded = base64.b64encode(proc.stdout).decode("ascii")
    return (
        "<div class=\"graphviz\">"
        f"<img src=\"data:image/svg+xml;base64,{encoded}\" alt=\"graphviz diagram\" />"
        "</div>"
    )


# Source-code renderers — register a handful so common file types
# get syntax highlighting via the prism CDN script (see templates).
# The server-side job is just to wrap the body in <pre><code
# class="language-<lang>"> so prism can pick it up.
_CODE_LANGS: dict[str, str] = {
    "text/x-python": "python",
    "text/x-go": "go",
    "text/x-rust": "rust",
    "text/x-javascript": "javascript",
    "application/javascript": "javascript",
    "text/x-typescript": "typescript",
    "text/x-shellscript": "bash",
    "text/x-sh": "bash",
    "text/x-yaml": "yaml",
    "application/x-yaml": "yaml",
    "text/x-toml": "toml",
    "text/x-c": "c",
    "text/x-csrc": "c",
    "text/x-c++src": "cpp",
    "text/x-java": "java",
    "text/css": "css",
    "text/html": "markup",
    "text/x-sql": "sql",
}


def _make_code_renderer(language: str) -> RendererFn:
    def _render(body: bytes, metadata: dict[str, Any]) -> str:
        text = html.escape(_decode(body))
        return f"<pre><code class=\"language-{language}\">{text}</code></pre>"
    _render.__name__ = f"_render_code_{language}"
    return _render


for _ct, _lang in _CODE_LANGS.items():
    register_renderer(_ct)(_make_code_renderer(_lang))


def registered_content_types() -> list[str]:
    """Snapshot of the renderer keys (test + introspection helper)."""
    with _REGISTRY_LOCK:
        keys = list(_REGISTRY.keys())
    return sorted(keys)
