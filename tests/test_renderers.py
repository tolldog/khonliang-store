"""Tests for renderer registry + built-ins."""

from __future__ import annotations

import shutil

import pytest

from store.viewer import renderers


def test_register_renderer_decorator_lookup():
    @renderers.register_renderer("test/widget")
    def _renderer(body, metadata):
        return f"<widget>{body.decode()}</widget>"

    out = renderers.render("test/widget", b"hi", {})
    assert out == "<widget>hi</widget>"


def test_render_strips_content_type_parameters():
    @renderers.register_renderer("test/withparams")
    def _renderer(body, metadata):
        return "<seen>"

    assert renderers.render("test/withparams; charset=utf-8", b"x", {}) == "<seen>"


def test_render_falls_back_to_raw_for_unknown_content_type():
    out = renderers.render("application/x-unknown-foo", b"<scary>&", {})
    # Defaults to text/plain renderer (the registered fallback).
    assert "<pre" in out
    assert "&lt;scary&gt;&amp;" in out  # html-escaped


def test_render_catches_renderer_exceptions():
    @renderers.register_renderer("test/raises")
    def _broken(body, metadata):
        raise RuntimeError("boom")

    out = renderers.render("test/raises", b"x", {})
    assert 'class="render-error"' in out
    assert "RuntimeError" in out
    assert "boom" in out


def test_json_renderer_pretty_prints():
    out = renderers.render(
        "application/json",
        b'{"a":1,"b":[2,3]}',
        {},
    )
    assert 'data-tree' in out
    # Output is HTML-escaped (quotes become &quot;) — assert the
    # indent + key shape rather than literal quotes.
    assert "&quot;a&quot;: 1" in out
    assert "&quot;b&quot;: [" in out


def test_json_renderer_falls_back_when_invalid_json():
    out = renderers.render("application/json", b"not actually json", {})
    # Falls through to raw <pre>, not to the error block.
    assert 'class="render-error"' not in out
    assert "not actually json" in out


def test_markdown_renderer_emits_data_markdown_block():
    out = renderers.render("text/markdown", b"# Hello\n\n- bullet", {})
    assert 'data-markdown' in out
    # Source preserved for the client-side renderer to read.
    assert "# Hello" in out


def test_code_renderer_wraps_in_prism_classes():
    out = renderers.render("text/x-python", b"def f():\n    return 1\n", {})
    assert 'class="language-python"' in out
    # Body html-escaped.
    assert "def f():" in out


def test_graphviz_renderer_emits_svg_when_dot_available():
    if shutil.which("dot") is None:
        pytest.skip("graphviz `dot` not installed")
    out = renderers.render(
        "text/vnd.graphviz",
        b"digraph G { a -> b }",
        {},
    )
    assert "<svg" in out
    assert 'class="graphviz"' in out


def test_graphviz_renderer_falls_back_when_dot_missing(monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda _: None)
    out = renderers.render(
        "text/vnd.graphviz",
        b"digraph G { a -> b }",
        {},
    )
    assert "graphviz-fallback" in out
    assert "digraph G" in out


def test_registered_content_types_includes_built_ins():
    types = renderers.registered_content_types()
    assert "text/plain" in types
    assert "application/json" in types
    assert "text/markdown" in types
    assert "text/vnd.graphviz" in types
    assert "text/x-python" in types
