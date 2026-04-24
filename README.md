# khonliang-store

Bus-native store agent. Target eventual owner of the artifact backend
(currently served by the bus's `bus_artifact_*` MCP surface) and host
of a browser-based viewer mode for rendered artifacts.

## Status — Phase 1 (scaffold)

Registered-but-empty shell. The agent connects to the bus and
declares no subclass skills yet. The inherited `health_check`
handler from `BaseAgent` remains dispatchable for liveness probes,
but is not listed in `registration.skills` — the test harness builds
registration from the subclass's `register_skills()` return, which
skips `BaseAgent.BUILT_IN_SKILLS`. Real functionality lands in
follow-up FRs one skill at a time so the infrastructure work
(repo, pyproject, tests, CLI) doesn't get re-litigated every
round.

Tracked under `fr_store_4ea7d48b`. Sibling FR `fr_store_d22556bb`
covers the viewer mode once artifact read skills are in place.

## Eventual scope

- **Artifact read skills**: `get`, `list`, `metadata`, `head`, `tail`,
  `grep`, `excerpt`. Same shape as `bus_artifact_*` today, but owned
  by the store agent.
- **Artifact write skills**: `stage_payload`, `replace`, `delete`,
  with provenance + content-type tracking.
- **Viewer mode**: `display(artifacts)` returns an ephemeral URL that
  renders each artifact (graphviz → SVG, markdown → HTML, JSON →
  collapsible tree, code → syntax-highlighted) in a tabbed or
  side-by-side browser view.
- **Migration path**: once the store agent owns the backend, the bus
  artifact surface becomes either a proxy or is deprecated in favor
  of direct `store-*` skills.

Everything above is deliberately **out of scope for Phase 1**. No
artifact ownership, no viewer, no migration — just a bus-registered
participant with nothing to do yet.

## Architecture boundary

- **khonliang-bus-lib** — agent SDK. `BaseAgent` + `@handler` +
  `AgentTestHarness` come from here.
- **khonliang-bus** — the bus service. Store registers as a
  participant; it does not run the bus itself.
- **khonliang-developer** / **khonliang-reviewer** — sibling agents.
  They consume artifacts today; once store owns the backend, they'll
  route through the store agent's skills instead of
  `bus_artifact_*`.

Store intentionally has **no** dependencies on khonliang-developer or
khonliang-reviewer. If a cross-agent interaction is needed, it goes
through the bus like every other agent-to-agent call.

## Running

```bash
# Install into the bus
python -m store.agent install --id store-primary --bus http://localhost:8788 --config config.yaml

# Start (normally done by the bus on boot)
python -m store.agent --id store-primary --bus http://localhost:8788 --config config.yaml

# Uninstall
python -m store.agent uninstall --id store-primary --bus http://localhost:8788
```

Copy `config.example.yaml` to `config.yaml` and edit for your local
bus URL. The config file is git-ignored.

## Development

```bash
pip install -e ".[test]"
pytest -q
python -m compileall store
```

Per the khonliang convention:
- Each change goes through a PR with Copilot review.
- Local `config.yaml` + `data/` + `logs/` are git-ignored.
- Commit messages reference the FR id where applicable.
