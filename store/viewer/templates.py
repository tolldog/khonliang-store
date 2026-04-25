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


# Pinned CDN URLs + SRI hashes — bumping deliberately is preferable
# to silent upgrades that change rendering behavior under us, and
# the integrity attribute means a compromised CDN can't substitute
# different bytes without the browser refusing to load them.
# Hashes are sha384, the standard SRI form jsdelivr publishes.
# When bumping versions, regenerate via:
#   curl -s <url> | openssl dgst -sha384 -binary | base64
_MARKED_CDN = "https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"
_MARKED_SRI = "sha384-/TQbtLCAerC3jgaim+N78RZSDYV7ryeoBCVqTuzRrFec2akfBkHS7ACQ3PQhvMVi"
_PRISM_CDN_CSS = "https://cdn.jsdelivr.net/npm/prismjs@1.29.0/themes/prism.min.css"
_PRISM_CSS_SRI = "sha384-rCCjoCPCsizaAAYVoz1Q0CmCTvnctK0JkfCSjx7IIxexTBg+uCKtFYycedUjMyA2"
_PRISM_CDN_JS = "https://cdn.jsdelivr.net/npm/prismjs@1.29.0/prism.min.js"
_PRISM_JS_SRI = "sha384-ZM8fDxYm+GXOWeJcxDetoRImNnEAS7XwVFH5kv0pT6RXNy92Nemw/Sj7NfciXpqg"
_PRISM_AUTOLOADER = (
    "https://cdn.jsdelivr.net/npm/prismjs@1.29.0/plugins/autoloader/prism-autoloader.min.js"
)
_PRISM_AUTOLOADER_SRI = (
    "sha384-Uq05+JLko69eOiPr39ta9bh7kld5PKZoU+fF7g0EXTAriEollhZ+DrN8Q/Oi8J2Q"
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
.tab .tab-label {
  border: none;
  background: transparent;
  color: inherit;
  font: inherit;
  cursor: pointer;
  padding: 0;
}
.tab .close {
  border: none;
  background: transparent;
  color: var(--muted);
  font-size: 14px;
  cursor: pointer;
  padding: 0 2px;
}
.tab .close:hover { color: var(--error); }
.tab.close-failed { border-color: var(--error); }
.json-tree { cursor: pointer; }
.json-tree.collapsed { max-height: 8em; overflow: hidden; }
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
      btn.disabled = true;
      fetch('/view/' + session + '/tab/' + tabId, { method: 'DELETE' })
        .then(function (resp) {
          if (!resp.ok) {
            // Server didn't drop it — leave the DOM alone so the
            // user sees a stable view instead of a phantom-removed
            // tab they can't interact with.
            btn.disabled = false;
            btn.title = 'close failed: HTTP ' + resp.status;
            tab.classList.add('close-failed');
            return;
          }
          tab.remove();
          var pane = document.querySelector('.pane[data-tab="' + tabId + '"]');
          if (pane) pane.remove();
          var first = document.querySelector('.tab');
          if (first) activate(first.dataset.tab);
        })
        .catch(function (err) {
          btn.disabled = false;
          btn.title = 'close failed: ' + err;
          tab.classList.add('close-failed');
        });
    });
  });
  // Render markdown blocks via marked. Apply the documented options
  // (mangle / headerIds off — keeps output deterministic + avoids
  // marked's built-in id-mangling); raw-HTML passthrough is curbed
  // by the post-strip below since marked@12 doesn't expose a
  // sanitize knob anymore.
  if (typeof marked !== 'undefined') {
    if (typeof marked.use === 'function') {
      marked.use({
        async: false,
        breaks: true,
        gfm: true,
        mangle: false,
        headerIds: false,
      });
    }
    document.querySelectorAll('[data-markdown]').forEach(function (host) {
      var src = host.querySelector('[data-source]');
      if (src) {
        var raw = src.textContent;
        var out = document.createElement('div');
        // textContent (not innerHTML) for the rendered output
        // would be too aggressive — we want headings, lists, links
        // to render as HTML. Strip <script> and on-handlers from
        // the marked output before injection. This is best-effort
        // belt-and-suspenders; the upstream guarantee is that we
        // own all artifacts in a local-trusted env.
        var html = marked.parse(raw);
        html = html.replace(/<script[\\s\\S]*?<\\/script>/gi, '');
        html = html.replace(/ on[a-z]+\\s*=\\s*("[^"]*"|'[^']*'|[^\\s>]+)/gi, '');
        // Neutralize `javascript:` URI schemes in href/src
        // attributes — even with the CSP, an inline `<a href=
        // "javascript:...">` would still execute on click.
        html = html.replace(
          /(href|src)\\s*=\\s*("\\s*javascript\\s*:[^"]*"|'\\s*javascript\\s*:[^']*'|\\s*javascript\\s*:[^\\s>]*)/gi,
          '$1="#"'
        );
        out.innerHTML = html;
        host.appendChild(out);
      }
    });
  }
  // Light JSON-tree decoration: collapse braces/brackets so deep
  // structures don't dominate the viewport. The renderer outputs
  // pretty-printed JSON inside <pre data-tree>; we wire a
  // click-to-toggle on top.
  document.querySelectorAll('pre[data-tree]').forEach(function (pre) {
    pre.classList.add('json-tree');
    pre.addEventListener('click', function () {
      pre.classList.toggle('collapsed');
    });
  });
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
        # <button> for the tab itself + a sibling <button> for the
        # close action — anchors without href aren't keyboard-
        # focusable, and nesting <button> inside <a> is invalid
        # HTML and confuses screen readers.
        tab_html_parts.append(
            f"<span class=\"tab\" data-tab=\"{html.escape(tab_id)}\" "
            f"data-session=\"{html.escape(session_id)}\">"
            f"<button type=\"button\" class=\"tab-label\">{label}</button>"
            f"<button type=\"button\" class=\"close\" title=\"close tab\" "
            f"aria-label=\"close tab\">×</button>"
            f"</span>"
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
        marked_sri=_MARKED_SRI,
        prism_css=_PRISM_CDN_CSS,
        prism_css_sri=_PRISM_CSS_SRI,
        prism_js=_PRISM_CDN_JS,
        prism_js_sri=_PRISM_JS_SRI,
        prism_autoloader=_PRISM_AUTOLOADER,
        prism_autoloader_sri=_PRISM_AUTOLOADER_SRI,
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
        marked_sri=_MARKED_SRI,
        prism_css=_PRISM_CDN_CSS,
        prism_css_sri=_PRISM_CSS_SRI,
        prism_js=_PRISM_CDN_JS,
        prism_js_sri=_PRISM_JS_SRI,
        prism_autoloader=_PRISM_AUTOLOADER,
        prism_autoloader_sri=_PRISM_AUTOLOADER_SRI,
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
<link rel="stylesheet" href="{prism_css}" integrity="{prism_css_sri}" crossorigin="anonymous">
<style>{css}</style>
</head>
<body data-meta="{session_meta}">
<nav id="tabs" class="layout-{layout_class}">{tabs_html}</nav>
<main id="panes">{panes_html}</main>
<script src="{marked_cdn}" integrity="{marked_sri}" crossorigin="anonymous"></script>
<script src="{prism_js}" integrity="{prism_js_sri}" crossorigin="anonymous"></script>
<script src="{prism_autoloader}" integrity="{prism_autoloader_sri}" crossorigin="anonymous"></script>
<script>{js}</script>
</body>
</html>
"""
