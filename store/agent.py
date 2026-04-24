"""Store agent — Phase 1 scaffold.

Registers on the bus with the minimum needed to be a participant:
the subclass declares zero skills. The built-in ``health_check``
handler inherited from :class:`khonliang_bus.BaseAgent` stays
dispatchable (routed through ``_handlers``) for liveness probes, but
is not listed in the bus's advertised skill set — that set comes
from ``register_skills()`` which returns ``[]`` here. The point of
this phase is to establish the repo, the install/uninstall CLI
pattern, and the registration surface so follow-up FRs can add real
skills one at a time without re-litigating the scaffolding.

Current scope (``fr_store_4ea7d48b``):
    - Class exists, subclass of :class:`BaseAgent`, ``agent_type = "store"``.
    - ``main()`` supports ``install`` / ``uninstall`` / run, matching
      the developer and reviewer agents.
    - Tests cover registration metadata + the built-in health_check.

Future scope (separate FRs):
    - Artifact read skills: get, list, metadata, head, tail, grep,
      excerpt. Matches the shape of the current ``bus_artifact_*``
      surface.
    - Artifact write skills: stage_payload, replace, delete.
    - Viewer mode (``fr_store_d22556bb``): ``display(artifacts)`` →
      ephemeral browser URL with tabbed/split layouts.
    - Migration path away from the bus-owned artifact backend.

Usage::

    # Install into the bus
    python -m store.agent install --id store-primary --bus http://localhost:8788 --config config.yaml

    # Start (normally done by the bus on boot)
    python -m store.agent --id store-primary --bus http://localhost:8788 --config config.yaml

    # Uninstall
    python -m store.agent uninstall --id store-primary --bus http://localhost:8788 --config config.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from khonliang_bus import BaseAgent


class StoreAgent(BaseAgent):
    """Bus-native store agent (Phase 1: scaffold only).

    No skills of its own — inherits ``health_check`` from
    :class:`BaseAgent`. Subsequent FRs add artifact + viewer surfaces.
    """

    agent_id = "store-primary"
    agent_type = "store"
    module_name = "store.agent"


def main() -> None:
    from khonliang_bus import add_version_flag

    # Omit `prog` so argparse derives it from argv[0]. The package
    # installs a `khonliang-store` console script AND is runnable via
    # `python -m store.agent`; hard-coding one name makes the
    # --help output misleading when invoked via the other path.
    parser = argparse.ArgumentParser(
        description="khonliang-store bus agent",
    )
    add_version_flag(parser)
    parser.add_argument(
        "command",
        nargs="?",
        choices=["install", "uninstall"],
        help="install or uninstall from the bus",
    )
    parser.add_argument("--id", default="store-primary")
    parser.add_argument("--bus", default="http://localhost:8788")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    if args.command in ("install", "uninstall"):
        StoreAgent.from_cli(
            [
                args.command,
                "--id", args.id,
                "--bus", args.bus,
                "--config", args.config,
            ]
        )
        return

    agent = StoreAgent(
        agent_id=args.id,
        bus_url=args.bus,
        config_path=args.config,
    )
    asyncio.run(agent.start())


if __name__ == "__main__":
    main()
