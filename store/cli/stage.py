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
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Iterable, Iterator

DEFAULT_STAGING_ROOT = Path("/var/lib/khonliang/staging")
ENV_STAGING_ROOT = "KHONLIANG_STAGING_ROOT"

EXIT_OK = 0
EXIT_USER_ERROR = 1
EXIT_NOT_IMPLEMENTED = 2


def staging_root() -> Path:
    """Return the active staging root.

    An *empty* ``KHONLIANG_STAGING_ROOT`` is treated as **unset** and
    falls through to the default. ``Path("")`` resolves to the current
    working directory, which would silently scatter bundles into
    whatever dir the caller happened to be in — a real footgun for
    shell scripts that pass through an unset env var as the empty
    string.
    """
    raw = os.environ.get(ENV_STAGING_ROOT, "")
    if raw == "":
        return DEFAULT_STAGING_ROOT
    return Path(raw)


def _capture_staged_diff(cwd: Path) -> bytes:
    """Run ``git diff --cached`` at ``cwd`` and return the raw bytes.

    Raises :class:`RuntimeError` for both git-not-found (FileNotFoundError)
    and any other ``OSError`` so that ``main()``'s single ``RuntimeError``
    branch surfaces a clean ``kh-stage:`` envelope on stderr instead of
    a traceback. The git-not-installed case in particular is common in
    minimal containers and CI images, and a traceback there is hostile.
    """
    try:
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
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"git executable not found on PATH: {exc}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(
            f"git subprocess failed to start at {cwd}: {exc}"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f"git diff --cached failed ({proc.returncode}) at {cwd}: "
            f"{proc.stderr.decode('utf-8', errors='replace').strip()}"
        )
    return proc.stdout


_STAGING_ROOT_MODE = 0o2770  # setgid + rwx for owner+group: any user in the
# khonliang group can create their own bundle under here (matches the
# documented install mode for /var/lib/khonliang/staging/).
_BUNDLE_DIR_MODE = 0o2750  # setgid + rwx for owner, rx for group: the bundle
# itself is owner-write only; the service-user reads via group membership.
_BUNDLE_FILE_MODE = 0o640  # rw for owner, r for group, none for other


@contextlib.contextmanager
def _no_umask() -> Iterator[None]:
    """Run with umask=0 so explicit mkdir/open mode bits land verbatim.

    Without this, ``os.mkdir(path, mode=0o2750)`` under a typical
    ``0o022`` umask still drops world bits at *create* time — fine
    when the explicit mode already excludes them — but more dangerous
    is the opposite case: a restrictive process umask (e.g. ``0o077``)
    would silently strip the group ``rx`` we *want* to keep, breaking
    the documented group-readable model. Suspending umask makes the
    mode arg the single source of truth.

    Process-global / not thread-safe; acceptable for a one-shot CLI.
    """
    prev = os.umask(0o000)
    try:
        yield
    finally:
        os.umask(prev)


def _restrictive_opener(file: str, flags: int) -> int:
    """``open(..., opener=...)`` hook that creates files at ``0640``.

    Setting the mode at ``os.open`` time (rather than after a default
    ``0o666 & ~umask`` create + later ``chmod``) closes the
    briefly-world-readable window flagged by review.
    """
    return os.open(file, flags, _BUNDLE_FILE_MODE)


def _write_fs_bundle(kind: str, files: dict[str, bytes], *, root: Path) -> str:
    """Write a bundle dir under ``root`` atomically with restrictive perms.

    Bundle is built under ``<root>/.tmp-<uuid>/`` first and renamed to
    ``<root>/<uuid>/`` only after every file is written, so a partial
    write (disk full, interrupt, perm error) never leaves a half-built
    bundle that a consumer might pick up.

    Permissions are applied at create time — not post-hoc via
    ``chmod`` — to eliminate the briefly-world-readable window:

    - The root dir, when this function has to bootstrap it, is created
      at ``2770`` so any user in the ``khonliang`` group can stage
      their own bundles (matches the documented install mode for
      ``/var/lib/khonliang/staging/``). In production the install
      step or tmpfiles.d has already created it; this branch is for
      dev/test where ``KHONLIANG_STAGING_ROOT`` points at a tmp dir.
    - Bundle dirs themselves are created at ``2750``: owner writes,
      service-user reads via the inherited group.
    - Bundle files are created at ``0640`` via ``os.open`` with
      ``mode=...``. Both the dir mkdir and the file open run under
      ``_no_umask()`` because ``os.open(mode=...)`` is still subject
      to the process umask — without suspension a restrictive caller
      umask (e.g. ``0o077``) would strip the group bit at create time
      and break the group-readable contract.
    """
    with _no_umask():
        root_existed = root.exists()
        root.mkdir(mode=_STAGING_ROOT_MODE, parents=True, exist_ok=True)
        if not root_existed:
            # Same setgid-bit-stripping behavior as the bundle dir
            # mkdir below; re-apply via chmod. Skip when the root
            # already existed so we don't overwrite a deliberately-
            # different mode on the production root (the install
            # script owns root permissions).
            os.chmod(root, _STAGING_ROOT_MODE)
    bundle_id = str(uuid.uuid4())
    tmp_dir = root / f".tmp-{bundle_id}"
    final_dir = root / bundle_id
    # Suspend umask for the entire bundle write — mkdir AND every file
    # open need umask=0 for their mode args to land verbatim.
    with _no_umask():
        os.mkdir(tmp_dir, mode=_BUNDLE_DIR_MODE)
        # ``mkdir(2)`` is allowed by POSIX to silently strip the
        # setgid bit, and Linux usually does. The world bits are
        # already excluded by ``_BUNDLE_DIR_MODE``, so the only
        # "before chmod" gap is the setgid bit (group inheritance) —
        # not any read/write access. Re-apply via ``chmod`` so files
        # written under this dir inherit the parent group as
        # documented; the gap is empty in practice (this function is
        # the only writer to its own temp dir) but the call is cheap.
        os.chmod(tmp_dir, _BUNDLE_DIR_MODE)
        try:
            manifest = {
                "kind": kind,
                "created_at": time.time(),
                "files": sorted(files.keys()),
            }
            with open(
                tmp_dir / "manifest.json",
                "w",
                encoding="utf-8",
                opener=_restrictive_opener,
            ) as mf:
                mf.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            for name, content in files.items():
                # Today (--diff only), ``files`` is always
                # ``{"diff.patch": ...}`` — no separators. A previous
                # iteration had a conditional nested-dir mkdir for the
                # future richer modes (changed-files / module / repo /
                # with-context, see fr_khonliang-bus-lib_520ce3bf), but
                # that branch was dead code AND silently broken: the
                # nested mkdir would have lost setgid via mkdir(2)'s
                # POSIX-undefined behavior with no chmod follow-up,
                # breaking the group-inheritance contract for files
                # written under nested paths. Reject separators up
                # front; when nested layouts ship, the writer of that
                # mode is responsible for adding back proper nested
                # mkdir + chmod handling (and tests).
                if "/" in name or "\\" in name:
                    raise ValueError(
                        f"_write_fs_bundle: nested paths in `files` are "
                        f"not supported yet (got {name!r}); see "
                        "fr_khonliang-bus-lib_520ce3bf changed-files mode"
                    )
                target = tmp_dir / name
                with open(target, "wb", opener=_restrictive_opener) as f:
                    f.write(content)
            os.rename(tmp_dir, final_dir)
        except BaseException:
            # Best-effort cleanup; re-raise so the caller sees the real failure.
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

    try:
        bundle_id = _write_fs_bundle(
            kind="diff",
            files={"diff.patch": diff_bytes},
            root=staging_root(),
        )
    except OSError as exc:
        # Stable script-friendly envelope for the common filesystem
        # failure modes: PermissionError (caller not in khonliang
        # group, or staging root unwritable), FileExistsError (UUID
        # collision — astronomically unlikely but cheaper than a
        # traceback if it ever happens), disk-full, rename-across-
        # filesystems, etc. Truly unexpected exceptions still bubble.
        print(f"kh-stage: bundle write failed: {exc}", file=sys.stderr)
        return EXIT_USER_ERROR
    print(f"fs:{bundle_id}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
