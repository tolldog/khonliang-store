"""HTTP-level integration tests for the viewer server.

These tests bind on 127.0.0.1 with port 0 (OS-assigned) and exercise
the actual ThreadingHTTPServer rather than mocking the request
handler. Each test gets a fresh registry and shuts the server down
on teardown.
"""

from __future__ import annotations

from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from store.viewer import PreparedTab, display
from store.viewer import server as viewer_server
from store.viewer.state import ArtifactRef


@pytest.fixture(autouse=True)
def _reset_viewer():
    viewer_server._reset_for_tests()
    yield
    viewer_server._reset_for_tests()


def _start_localhost_viewer():
    """Start a viewer bound to 127.0.0.1 on a random port."""
    return viewer_server.ensure_server(host="127.0.0.1", port=0)


def test_get_session_renders_tab():
    _start_localhost_viewer()
    result = display(
        [
            PreparedTab(
                artifact=ArtifactRef(id="art_a", view_hint="hello"),
                content_type="text/markdown",
                body=b"# Hello",
            ),
        ],
    )
    with urlopen(result["url"], timeout=2) as resp:
        # Capture status + body inside the with-block; using a
        # closed urllib response object afterward is implementation-
        # defined across Python versions.
        status = resp.status
        body = resp.read().decode("utf-8")
    assert status == 200
    assert 'data-markdown' in body
    assert "# Hello" in body
    # Tab label uses the view_hint.
    assert ">hello<" in body


def test_unknown_session_returns_404():
    server = _start_localhost_viewer()
    with pytest.raises(HTTPError) as ei:
        urlopen(f"{server.base_url}/view/does-not-exist", timeout=2)
    assert ei.value.code == 404


def test_healthz():
    server = _start_localhost_viewer()
    with urlopen(f"{server.base_url}/healthz", timeout=2) as resp:
        assert resp.status == 200
        assert resp.read() == b"ok"


def test_delete_tab_drops_only_that_tab():
    _start_localhost_viewer()
    result = display(
        [
            PreparedTab(
                artifact=ArtifactRef(id="a"),
                content_type="text/plain",
                body=b"first",
            ),
            PreparedTab(
                artifact=ArtifactRef(id="b"),
                content_type="text/plain",
                body=b"second",
            ),
        ],
    )
    server = viewer_server._SERVER
    assert server is not None
    drop_url = f"{server.base_url}/view/{result['session_id']}/tab/{result['tab_ids'][0]}"
    req = Request(drop_url, method="DELETE")
    with urlopen(req, timeout=2) as resp:
        assert resp.status == 204
    session = server.registry.get_session(result["session_id"])
    assert session is not None
    assert result["tab_ids"][0] not in session.tabs
    assert result["tab_ids"][1] in session.tabs


def test_delete_session_drops_session():
    _start_localhost_viewer()
    result = display(
        [
            PreparedTab(
                artifact=ArtifactRef(id="a"),
                content_type="text/plain",
                body=b"x",
            ),
        ],
    )
    server = viewer_server._SERVER
    assert server is not None
    req = Request(
        f"{server.base_url}/view/{result['session_id']}",
        method="DELETE",
    )
    with urlopen(req, timeout=2) as resp:
        assert resp.status == 204
    assert server.registry.get_session(result["session_id"]) is None
