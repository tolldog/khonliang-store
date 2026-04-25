"""Tests for the in-memory session/tab registry."""

from __future__ import annotations

from store.viewer.state import ArtifactRef, SessionRegistry


def test_create_session_returns_unique_ids():
    reg = SessionRegistry()
    a = reg.create_session()
    b = reg.create_session()
    assert a.session_id and b.session_id
    assert a.session_id != b.session_id


def test_add_tab_attaches_to_session_and_returns_unique_id():
    reg = SessionRegistry()
    s = reg.create_session()
    t1 = reg.add_tab(
        s.session_id,
        ArtifactRef(id="art_a"),
        content_type="text/plain",
        body=b"hello",
    )
    t2 = reg.add_tab(
        s.session_id,
        ArtifactRef(id="art_b"),
        content_type="text/markdown",
        body=b"# hi",
    )
    assert t1.tab_id != t2.tab_id
    fetched = reg.get_session(s.session_id)
    assert fetched is not None
    assert set(fetched.tabs) == {t1.tab_id, t2.tab_id}


def test_add_tab_rejects_unknown_session():
    import pytest
    reg = SessionRegistry()
    with pytest.raises(KeyError):
        reg.add_tab(
            "nope", ArtifactRef(id="art_a"),
            content_type="text/plain", body=b"",
        )


def test_drop_tab_removes_only_that_tab():
    reg = SessionRegistry()
    s = reg.create_session()
    t1 = reg.add_tab(s.session_id, ArtifactRef(id="a"), content_type="text/plain", body=b"")
    t2 = reg.add_tab(s.session_id, ArtifactRef(id="b"), content_type="text/plain", body=b"")
    assert reg.drop_tab(s.session_id, t1.tab_id) is True
    fetched = reg.get_session(s.session_id)
    assert fetched is not None
    assert set(fetched.tabs) == {t2.tab_id}


def test_drop_tab_returns_false_when_unknown():
    reg = SessionRegistry()
    s = reg.create_session()
    assert reg.drop_tab(s.session_id, "ghost") is False
    assert reg.drop_tab("ghost-session", "ghost") is False


def test_drop_session_removes_session_entirely():
    reg = SessionRegistry()
    s = reg.create_session()
    reg.add_tab(s.session_id, ArtifactRef(id="a"), content_type="text/plain", body=b"")
    assert reg.drop_session(s.session_id) is True
    assert reg.get_session(s.session_id) is None


def test_snapshot_reports_counts():
    reg = SessionRegistry()
    s1 = reg.create_session()
    s2 = reg.create_session()
    reg.add_tab(s1.session_id, ArtifactRef(id="a"), content_type="text/plain", body=b"")
    reg.add_tab(s1.session_id, ArtifactRef(id="b"), content_type="text/plain", body=b"")
    reg.add_tab(s2.session_id, ArtifactRef(id="c"), content_type="text/plain", body=b"")
    snap = reg.snapshot()
    assert snap == {"session_count": 2, "tab_count": 3}
