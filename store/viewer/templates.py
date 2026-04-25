"""HTML templates for the viewer.

Stdlib-only; client-side dependencies (marked.js for markdown,
prism.js for code highlighting) are loaded from CDNs. Per the FR:
"Decoration: minimal — tab bar + content pane, no navigation tree,
no search."
"""

from __future__ import annotations

import html
import json
from typing import Iterable, Mapping


# Pinned CDN URLs — bumping deliberately is preferable to silent
# upgrades that change rendering behavior under us.
_MARKED_CDN = "https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"
_PRISM_CDN_CSS = "https://cdn.jsdelivr.net/npm/prismjs@1.29.0/themes/prism.min.css"
_PRISM_CDN_JS = "https://cdn.jsdelivr.net/npm/prismjs@1.29.0/prism.min.js"
_PRISM_AUTOLOADER = (
    "https://cdn.jsdelivr.net/npm/prismjs@1.29.0/plugins/autoloader/prism-autoloader.min.js"
)


_CSS = """
:root {
  --fg: #1f2933;
  --muted: #52606d;
  --bg: #ffffff;
  --bg-alt: #f5f7fa;
  --accent: #2563eb;
  --border: #cbd5e1;
  --error: #b91c1c;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  color: var(--fg);
  background: var(--bg);
}
#tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 2px;
  padding: 8px;
  background: var(--bg-alt);
  border-bottom: 1px solid var(--border);
  position: sticky;
  top: 0;
  z-index: 1;
}
.tab {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border: 1px solid var(--border);
  border-radius: 4px;
  background: var(--bg);
  color: var(--muted);
  text-decoration: none;
  cursor: pointer;
  font-size: 12px;
}
.tab.active { color: var(--accent); border-color: var(--accent); font-weight: 600; }
.tab .close {
  border: none;
  background: transparent;
  color: var(--muted);
  font-size: 14px;
  cursor: pointer;
  padding: 0 2px;
}
.tab .close:hover { color: var(--error); }
#panes { padding: 12px 16px; }
.pane { display: none; }
.pane.active { display: block; }
.pane pre { background: var(--bg-alt); padding: 12px; overflow: auto; border-radius: 4px; }
.pane.split-active { display: flex; gap: 12px; }
.pane.split-active > * { flex: 1; min-width: 0; overflow: auto; }
.markdown pre[data-source] { display: none; }
.render-error { color: var(--error); padding: 12px; border: 1px solid var(--error); border-radius: 4px; }
#empty { padding: 24px; color: var(--muted); }
"""


_JS = (
    """
(function () {
  function activate(tabId) {
    document.querySelectorAll('.tab').forEach(function (el) {
      el.classList.toggle('active', el.dataset.tab === tabId);
    });
    document.querySelectorAll('.pane').forEach(function (el) {
      el.classList.toggle('active', el.dataset.tab === tabId);
    });
  }
  document.querySelectorAll('.tab').forEach(function (el) {
    el.addEventListener('click', function (e) {
      if (e.target.classList.contains('close')) return;
      activate(el.dataset.tab);
    });
  });
  document.querySelectorAll('.tab .close').forEach(function (btn) {
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      var tab = btn.closest('.tab');
      var session = tab.dataset.session;
      var tabId = tab.dataset.tab;
      fetch('/view/' + session + '/tab/' + tabId, { method: 'DELETE' }).then(function () {
        tab.remove();
        var pane = document.querySelector('.pane[data-tab="' + tabId + '"]');
        if (pane) pane.remove();
        var first = document.querySelector('.tab');
        if (first) activate(first.dataset.tab);
      });
    });
  });
  // Render markdown blocks via marked, if loaded.
  if (typeof marked !== 'undefined') {
    document.querySelectorAll('[data-markdown]').forEach(function (host) {
      var src = host.querySelector('[data-source]');
      if (src) {
        var out = document.createElement('div');
        out.innerHTML = marked.parse(src.textContent);
        host.appendChild(out);
      }
    });
  }
  // Activate first tab on load.
  var first = document.querySelector('.tab');
  if (first) activate(first.dataset.tab);
})();
"""
)


def _tab_label(tab: object) -> str:
    """Short tab title — falls back to a truncated artifact id."""
    art = getattr(tab, "artifact", None)
    if art is None:
        return getattr(tab, "tab_id", "?")
    hint = (getattr(art, "view_hint", "") or "").strip()
    if hint:
        return hint
    return getattr(art, "id", "?")[:24]


def render_session_page(
    session_id: str,
    *,
    layout: str,
    tabs: Iterable[object],
    rendered_panes: Mapping[str, str],
) -> str:
    """Compose the full HTML for a session's view URL.

    ``rendered_panes`` is keyed by tab_id and holds the HTML fragment
    each renderer produced. ``tabs`` is the iteration order.
    """
    tab_list = list(tabs)
    if not tab_list:
        return _empty_session_page(session_id)

    tab_html_parts: list[str] = []
    pane_html_parts: list[str] = []
    for tab in tab_list:
        tab_id = getattr(tab, "tab_id")
        label = html.escape(_tab_label(tab))
        tab_html_parts.append(
            f"<a class=\"tab\" data-tab=\"{html.escape(tab_id)}\" "
            f"data-session=\"{html.escape(session_id)}\">"
            f"<span>{label}</span>"
            f"<button class=\"close\" title=\"close tab\">×</button>"
            f"</a>"
        )
        body = rendered_panes.get(tab_id, "")
        pane_html_parts.append(
            f"<div class=\"pane\" data-tab=\"{html.escape(tab_id)}\">{body}</div>"
        )

    title = f"khonliang-store viewer — {len(tab_list)} tab(s)"
    layout_class = "split" if layout == "split" else "tabs"
    return _PAGE_SHELL.format(
        title=html.escape(title),
        css=_CSS,
        marked_cdn=_MARKED_CDN,
        prism_css=_PRISM_CDN_CSS,
        prism_js=_PRISM_CDN_JS,
        prism_autoloader=_PRISM_AUTOLOADER,
        layout_class=html.escape(layout_class),
        tabs_html="".join(tab_html_parts),
        panes_html="".join(pane_html_parts),
        session_meta=html.escape(json.dumps({"session_id": session_id, "layout": layout})),
        js=_JS,
    )


def _empty_session_page(session_id: str) -> str:
    return _PAGE_SHELL.format(
        title=html.escape("khonliang-store viewer — empty session"),
        css=_CSS,
        marked_cdn=_MARKED_CDN,
        prism_css=_PRISM_CDN_CSS,
        prism_js=_PRISM_CDN_JS,
        prism_autoloader=_PRISM_AUTOLOADER,
        layout_class="empty",
        tabs_html="",
        panes_html=(
            f"<div id=\"empty\">Session <code>{html.escape(session_id)}</code> "
            "has no tabs. Call <code>display(artifacts)</code> with at least one "
            "ArtifactRef.</div>"
        ),
        session_meta=html.escape(json.dumps({"session_id": session_id})),
        js="",
    )


_PAGE_SHELL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<link rel="stylesheet" href="{prism_css}">
<style>{css}</style>
</head>
<body data-meta="{session_meta}">
<nav id="tabs" class="layout-{layout_class}">{tabs_html}</nav>
<main id="panes">{panes_html}</main>
<script src="{marked_cdn}"></script>
<script src="{prism_js}"></script>
<script src="{prism_autoloader}"></script>
<script>{js}</script>
</body>
</html>
"""
