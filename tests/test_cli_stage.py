"""Tests for ``store.cli.stage`` (``kh-stage`` CLI).

Covers --diff + fs backend (the only wired path today). Reserved
depth flags and --backend=store should reject cleanly with nonzero
exit and stderr messages so the flag surface stays forward-stable.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from store.cli import stage


def _init_repo(path: Path) -> None:
    """Initialize a git repo with one staged change.

    Configures user identity locally (not globally) so the staged
    commit doesn't depend on the test environment's git config.
    """
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    subprocess.run(
        ["git", "-C", str(path), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(path), "config", "user.name", "Test"],
        check=True,
    )
    (path / "README").write_text("initial\n")
    subprocess.run(["git", "-C", str(path), "add", "README"], check=True)
    subprocess.run(
        ["git", "-C", str(path), "commit", "-q", "-m", "init"],
        check=True,
    )
    (path / "README").write_text("initial\nadded line\n")
    subprocess.run(["git", "-C", str(path), "add", "README"], check=True)


def test_diff_fs_happy_path(tmp_path: Path, monkeypatch, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)

    staging = tmp_path / "staging"
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(staging))

    rc = stage.main(["--diff", "--cwd", str(repo)])
    captured = capsys.readouterr()

    assert rc == stage.EXIT_OK
    assert captured.err == ""

    handle = captured.out.strip()
    assert handle.startswith("fs:")
    bundle_id = handle.split(":", 1)[1]

    bundle_dir = staging / bundle_id
    assert bundle_dir.is_dir()

    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    assert manifest["kind"] == "diff"
    assert manifest["files"] == ["diff.patch"]
    assert isinstance(manifest["created_at"], (int, float))

    diff_bytes = (bundle_dir / "diff.patch").read_bytes()
    assert b"+added line" in diff_bytes
    assert b"--- a/README" in diff_bytes


def test_empty_staged_diff_rejects(tmp_path: Path, monkeypatch, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"],
        check=True,
    )

    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(tmp_path / "staging"))

    rc = stage.main(["--diff", "--cwd", str(repo)])
    captured = capsys.readouterr()

    assert rc == stage.EXIT_USER_ERROR
    assert "no staged changes" in captured.err
    assert captured.out == ""


def test_non_git_cwd_rejects(tmp_path: Path, monkeypatch, capsys) -> None:
    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()

    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(tmp_path / "staging"))

    rc = stage.main(["--diff", "--cwd", str(not_a_repo)])
    captured = capsys.readouterr()

    assert rc == stage.EXIT_USER_ERROR
    assert "git diff --cached failed" in captured.err
    assert captured.out == ""


def test_store_backend_not_implemented(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(tmp_path / "staging"))

    rc = stage.main(["--diff", "--backend", "store"])
    captured = capsys.readouterr()

    assert rc == stage.EXIT_NOT_IMPLEMENTED
    assert "--backend=store" in captured.err
    assert "fr_reviewer_800e851d" in captured.err
    assert captured.out == ""


@pytest.mark.parametrize(
    "depth_flag",
    ["--changed-files", "--module", "--repo", "--with-context"],
)
def test_reserved_depth_flags_not_implemented(
    tmp_path: Path,
    monkeypatch,
    capsys,
    depth_flag: str,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(tmp_path / "staging"))

    rc = stage.main([depth_flag, "--cwd", str(repo)])
    captured = capsys.readouterr()

    assert rc == stage.EXIT_NOT_IMPLEMENTED
    assert "reserved but not yet implemented" in captured.err
    assert captured.out == ""


def test_missing_depth_flag_argparse_error(tmp_path: Path, capsys) -> None:
    with pytest.raises(SystemExit) as exc_info:
        stage.main([])
    assert exc_info.value.code != 0


def test_handle_format_is_uuid(tmp_path: Path, monkeypatch, capsys) -> None:
    """fs:<id> id should parse as a UUID — reviewer-side resolver will rely on it."""
    import uuid as _uuid

    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(tmp_path / "staging"))

    rc = stage.main(["--diff", "--cwd", str(repo)])
    handle = capsys.readouterr().out.strip()
    assert rc == stage.EXIT_OK

    bundle_id = handle.split(":", 1)[1]
    _uuid.UUID(bundle_id)
