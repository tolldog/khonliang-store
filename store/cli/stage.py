"""``kh-stage``: stage byte-shaped agent inputs and emit a routable handle.

Pattern: the caller (a Claude session, a shell user, a hook) wants an
agent (reviewer, etc.) to act on a large blob. Instead of routing the
bytes through the caller's context (token cost) or asking the agent to
read into a path the service user can't reach (permission failure),
``kh-stage`` writes the bundle to a group-readable staging dir and
prints a short handle. The agent resolves the handle on its side.

Handle shape: ``<backend>:<id>``.

  fs:<uuid>   — bundle under ``/var/lib/khonliang/staging/<uuid>/``
                (default backend today)
  store:<id>  — bundle stored as an artifact via the store agent
                (deferred; reserved flag, rejected today)

Implemented depth today:
  --diff   capture ``git diff --cached`` from ``--cwd`` (or $PWD)

Reserved depth flags (rejected with nonzero exit + stderr today;
flag surface forward-stable so reviewer-side resolver lands once):
  --changed-files / --module / --repo / --with-context

See ``fr_khonliang-bus-lib_520ce3bf`` (this CLI) and
``fr_reviewer_800e851d`` (the matching reviewer consumer).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Iterable

DEFAULT_STAGING_ROOT = Path("/var/lib/khonliang/staging")
ENV_STAGING_ROOT = "KHONLIANG_STAGING_ROOT"

EXIT_OK = 0
EXIT_USER_ERROR = 1
EXIT_NOT_IMPLEMENTED = 2


def staging_root() -> Path:
    return Path(os.environ.get(ENV_STAGING_ROOT, str(DEFAULT_STAGING_ROOT)))


def _capture_staged_diff(cwd: Path) -> bytes:
    proc = subprocess.run(
        [
            "git",
            "-C",
            str(cwd),
            "diff",
            "--cached",
            "--no-color",
            "--no-ext-diff",
            "--binary",
        ],
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"git diff --cached failed ({proc.returncode}) at {cwd}: "
            f"{proc.stderr.decode('utf-8', errors='replace').strip()}"
        )
    return proc.stdout


_BUNDLE_DIR_MODE = 0o2750  # setgid + rwx for owner, rx for group, none for other
_BUNDLE_FILE_MODE = 0o640  # rw for owner, r for group, none for other


def _write_fs_bundle(kind: str, files: dict[str, bytes], *, root: Path) -> str:
    """Write a bundle dir under ``root`` atomically.

    Bundle is built under ``<root>/.tmp-<uuid>/`` first and renamed to
    ``<root>/<uuid>/`` only after every file is written, so a partial
    write (disk full, interrupt, perm error) never leaves a half-built
    bundle that a consumer might pick up.

    Permissions are set explicitly rather than letting the process
    umask leak the bundle world-readable: dirs are ``2750`` (setgid so
    child files inherit the parent group), files are ``0640``.
    """
    root.mkdir(parents=True, exist_ok=True)
    bundle_id = str(uuid.uuid4())
    tmp_dir = root / f".tmp-{bundle_id}"
    final_dir = root / bundle_id
    tmp_dir.mkdir(parents=False, exist_ok=False)
    try:
        os.chmod(tmp_dir, _BUNDLE_DIR_MODE)
        manifest = {
            "kind": kind,
            "created_at": time.time(),
            "files": sorted(files.keys()),
        }
        manifest_path = tmp_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(manifest_path, _BUNDLE_FILE_MODE)
        for name, content in files.items():
            target = tmp_dir / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            os.chmod(target, _BUNDLE_FILE_MODE)
        os.rename(tmp_dir, final_dir)
    except BaseException:
        # Best-effort cleanup; re-raise so the caller sees the real failure.
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return bundle_id


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Stage byte-shaped agent inputs and emit a routable handle "
            "(<backend>:<id>) on stdout. See fr_khonliang-bus-lib_520ce3bf."
        ),
    )
    depth = p.add_mutually_exclusive_group(required=True)
    depth.add_argument(
        "--diff",
        action="store_const",
        const="diff",
        dest="depth",
        help="capture `git diff --cached` from --cwd",
    )
    depth.add_argument(
        "--changed-files",
        action="store_const",
        const="changed-files",
        dest="depth",
        help="(reserved; not yet implemented)",
    )
    depth.add_argument(
        "--module",
        action="store_const",
        const="module",
        dest="depth",
        help="(reserved; not yet implemented)",
    )
    depth.add_argument(
        "--repo",
        action="store_const",
        const="repo",
        dest="depth",
        help="(reserved; not yet implemented)",
    )
    depth.add_argument(
        "--with-context",
        action="store_const",
        const="with-context",
        dest="depth",
        help="(reserved; not yet implemented)",
    )
    p.add_argument(
        "--backend",
        default="fs",
        choices=("fs", "store"),
        help="staging substrate (default: fs)",
    )
    p.add_argument(
        "--cwd",
        default=None,
        help="working directory for git operations (default: $PWD)",
    )
    return p


def main(argv: Iterable[str] | None = None) -> int:
    args = _build_parser().parse_args(list(argv) if argv is not None else None)

    if args.backend == "store":
        print(
            "kh-stage: --backend=store is not yet implemented; "
            "lands with fr_reviewer_800e851d store: prefix dispatch",
            file=sys.stderr,
        )
        return EXIT_NOT_IMPLEMENTED

    if args.depth != "diff":
        print(
            f"kh-stage: depth --{args.depth} is reserved but not yet "
            "implemented (only --diff is wired today)",
            file=sys.stderr,
        )
        return EXIT_NOT_IMPLEMENTED

    cwd = Path(args.cwd) if args.cwd else Path.cwd()
    try:
        diff_bytes = _capture_staged_diff(cwd)
    except RuntimeError as exc:
        print(f"kh-stage: {exc}", file=sys.stderr)
        return EXIT_USER_ERROR

    if not diff_bytes:
        print(
            f"kh-stage: no staged changes at {cwd} (git diff --cached empty)",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR

    bundle_id = _write_fs_bundle(
        kind="diff",
        files={"diff.patch": diff_bytes},
        root=staging_root(),
    )
    print(f"fs:{bundle_id}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
