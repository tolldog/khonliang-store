"""Tests for ``store.cli.stage`` (``kh-stage`` CLI).

Covers --diff + fs backend (the only wired path today). Reserved
depth flags and --backend=store should reject cleanly with nonzero
exit and stderr messages so the flag surface stays forward-stable.
"""

from __future__ import annotations

import json
import os
import stat as stat_mod
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


def test_git_diff_invoked_with_deterministic_flags(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """The captured diff must be free of color codes, external-diff
    output, and complete for binary changes — i.e. ``--no-color
    --no-ext-diff --binary`` must always be on the git command line.
    Otherwise reviewer-side parsing breaks when the user has
    ``color.ui=always`` or a custom ``diff.external`` configured.
    """
    seen_args: list[list[str]] = []
    real_run = subprocess.run

    def spy_run(cmd, *args, **kwargs):
        seen_args.append(list(cmd))
        return real_run(cmd, *args, **kwargs)

    monkeypatch.setattr(stage.subprocess, "run", spy_run)

    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(tmp_path / "staging"))

    rc = stage.main(["--diff", "--cwd", str(repo)])
    assert rc == stage.EXIT_OK
    capsys.readouterr()

    diff_calls = [c for c in seen_args if "diff" in c and "--cached" in c]
    assert diff_calls, "expected at least one `git diff --cached` invocation"
    git_argv = diff_calls[0]
    assert "--no-color" in git_argv
    assert "--no-ext-diff" in git_argv
    assert "--binary" in git_argv


def test_bundle_permissions_are_restrictive(tmp_path: Path, monkeypatch, capsys) -> None:
    """Three distinct modes in the documented model:

    - staging root: ``2770`` (group-writable so multiple users in the
      ``khonliang`` group can each create their own bundle).
    - bundle dir:   ``2750`` (group-readable only; only the bundle
      owner writes inside).
    - bundle files: ``0640`` (group-readable only).
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    staging = tmp_path / "staging"
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(staging))

    rc = stage.main(["--diff", "--cwd", str(repo)])
    assert rc == stage.EXIT_OK
    handle = capsys.readouterr().out.strip()
    bundle_id = handle.split(":", 1)[1]
    bundle_dir = staging / bundle_id

    root_mode = stat_mod.S_IMODE(staging.stat().st_mode)
    assert root_mode == 0o2770, f"staging root mode {oct(root_mode)} != 0o2770"

    dir_mode = stat_mod.S_IMODE(bundle_dir.stat().st_mode)
    assert dir_mode == 0o2750, f"bundle dir mode {oct(dir_mode)} != 0o2750"

    for entry in bundle_dir.iterdir():
        f_mode = stat_mod.S_IMODE(entry.stat().st_mode)
        assert f_mode == 0o640, f"{entry.name} mode {oct(f_mode)} != 0o640"


def test_file_creation_immune_to_restrictive_umask(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Pass-3 finding: ``os.open(mode=0o640)`` is still masked by the
    process umask. A caller running with a restrictive umask (e.g.
    ``0o077``) would silently strip the group-read bit and break the
    group-readable contract. The fix wraps the file opens in
    ``_no_umask()``; this test pins it by setting umask to ``0o077``
    before invoking ``main()`` and asserting the bundle files still
    end up at exactly ``0o640``.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    staging = tmp_path / "staging"
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(staging))

    prev_umask = os.umask(0o077)
    try:
        rc = stage.main(["--diff", "--cwd", str(repo)])
    finally:
        os.umask(prev_umask)
    assert rc == stage.EXIT_OK
    handle = capsys.readouterr().out.strip()
    bundle_id = handle.split(":", 1)[1]
    bundle_dir = staging / bundle_id

    for entry in bundle_dir.iterdir():
        f_mode = stat_mod.S_IMODE(entry.stat().st_mode)
        assert f_mode == 0o640, (
            f"{entry.name} mode {oct(f_mode)} != 0o640 — opener mode "
            "was masked by the caller umask, breaking group-readable"
        )


def test_rename_failure_leaves_no_orphan(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """``os.rename`` is the last step of the atomic-write sequence; if
    it fails (cross-filesystem, perm error, race with another writer),
    the half-built temp dir must be cleaned up, ``main()`` must
    surface ``EXIT_USER_ERROR`` with a stderr envelope (not a
    traceback), and no final bundle dir must be exposed for a
    consumer to pick up.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    staging = tmp_path / "staging"
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(staging))

    real_rename = os.rename

    def boom_rename(src, dst, *args, **kwargs):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(stage.os, "rename", boom_rename)

    rc = stage.main(["--diff", "--cwd", str(repo)])
    captured = capsys.readouterr()

    # Findings #3: stable envelope, not a traceback bubbling out.
    assert rc == stage.EXIT_USER_ERROR
    assert "bundle write failed" in captured.err
    assert "simulated rename failure" in captured.err
    assert captured.out == ""

    # No final bundle dir exposed.
    final_entries = [
        p for p in staging.iterdir()
        if p.is_dir() and not p.name.startswith(".tmp-")
    ]
    assert final_entries == []

    # No temp dir left behind.
    tmp_entries = [
        p for p in staging.iterdir() if p.name.startswith(".tmp-")
    ]
    assert tmp_entries == [], f"orphan temp dirs: {tmp_entries}"

    # Sanity: the real rename is still around for other tests.
    assert real_rename is os.rename or real_rename is not None


def test_no_world_bits_ever_set_during_write(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Findings #1 + #2: world bits must never appear on any bundle
    path, not even transiently. The implementation uses umask=0 + an
    ``os.open`` opener so files/dirs are created at restrictive mode
    from the start. Any ``chmod`` calls inside the staging root must
    only set modes that are already free of world bits (a setgid
    re-apply is fine; promoting 0o0750 -> 0o2750 stays restrictive).
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_repo(repo)
    staging = tmp_path / "staging"
    monkeypatch.setenv(stage.ENV_STAGING_ROOT, str(staging))

    chmod_calls: list[tuple[str, int]] = []
    real_chmod = os.chmod

    def spy_chmod(path, mode, *args, **kwargs):
        chmod_calls.append((str(path), mode))
        return real_chmod(path, mode, *args, **kwargs)

    monkeypatch.setattr(stage.os, "chmod", spy_chmod)

    rc = stage.main(["--diff", "--cwd", str(repo)])
    assert rc == stage.EXIT_OK
    capsys.readouterr()

    staging_str = str(staging)
    for path, mode in chmod_calls:
        if staging_str in path:
            # Any chmod on a staging path must leave world bits cleared.
            assert mode & 0o007 == 0, (
                f"chmod({path!r}, {oct(mode)}) sets world bits"
            )

    # Final state still matches the documented mode model.
    bundle_dirs = [
        p for p in staging.iterdir()
        if p.is_dir() and not p.name.startswith(".tmp-")
    ]
    assert len(bundle_dirs) == 1
    bundle_dir = bundle_dirs[0]
    assert stat_mod.S_IMODE(bundle_dir.stat().st_mode) == 0o2750
    for entry in bundle_dir.iterdir():
        assert stat_mod.S_IMODE(entry.stat().st_mode) == 0o640
