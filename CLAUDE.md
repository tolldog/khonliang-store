# khonliang-store

Bus-native store agent. Eventual owner of the artifact backend and
host of a browser-based viewer mode. The phased-landing convention
is preserved — **don't bundle Phase 2 (artifact read skills) into a
Phase 4 (write ownership) PR or vice versa**. Each phase is its own
FR with its own PR.

## Status

Phase 5 (`fr_researcher_000ad07c`) shipped — researcher PR #34
(2026-04-26). `stage_payload` and `ingest_from_artifact` are
registered as bus skills on researcher-primary; the write side of
the artifact lifecycle now crosses cleanly between store (owns the
bytes) and researcher (consumes them as corpus).

Phase 4c (bus repo) shipped — bus PR #24 (2026-04-26).
`bus_artifact_*` read tools (`get`/`list`/`head`/`tail`/`metadata`/
`grep`/`excerpt`) were removed from the bus MCP adapter; only
`bus_artifact_distill[_many]` remain. The bus REST surface is
marked deprecated and slated for removal once all operators have
run `artifact_migrate_from_bus`.

Phase 4b (`fr_store_ef668d56`): `CompositeArtifactBackend(local,
bus)` — writes go to the local SQLite store; reads check local
first and fall through to the bus REST surface for any artifact
not yet migrated. New skill `artifact_migrate_from_bus(limit,
dry_run)` walks the bus's list endpoint and copies each
artifact into local SQLite preserving ids; idempotent re-run
skips already-present rows. Config knob `[artifacts] backend:
composite` opts in. Default still `bus` until operators have
run the migration in their environments.

Phase 4a (`fr_store_73e5a6f4`): `LocalArtifactStore` (SQLite-backed
implementation of `ArtifactBackend`) plus `artifact_create` skill.

Phase 2 reads (`fr_store_08c1c6b2`): the store agent owns the
artifact read surface — `artifact_list`, `artifact_metadata`,
`artifact_get`, `artifact_head`, `artifact_tail`, `artifact_grep`,
`artifact_excerpt`. All seven route through an `ArtifactBackend`
abstraction; the same abstraction now serves both
`BusBackedArtifactStore` (proxy) and `LocalArtifactStore` (own).

Phase 3 viewer (`fr_store_d22556bb`): `display(artifacts,
layout='tabs')` lazily starts an in-process HTTP viewer,
pre-fetches artifacts via the same `ArtifactBackend` (in-process
call, no bus round-trip), and returns a browser URL. Renderers
are extensible via `@register_renderer("type/x")`.

The phase roadmap is complete. New work in this repo is filed as
fresh FRs — most recently `fr_khonliang-bus-lib_520ce3bf` shipped
`kh-stage`, a user-side CLI that captures byte payloads (`git diff
--cached` today) and emits a routable handle for byte-shaped agent
input handoff (2026-05-30).

## Stack

- Python, async throughout
- SQLite-backed local artifact store (`LocalArtifactStore`,
  shipped in Phase 4a)
- Native khonliang-bus agent via `khonliang-bus-lib`

## Ecosystem position

```
INFRASTRUCTURE (services)
├─ khonliang-scheduler  — LLM inference scheduling
└─ khonliang-bus        — agent bus service, service registry,
                          MCP adapter (artifact backend handed off
                          to store agent in Phase 4)

LIBRARIES (Python)
├─ khonliang            — agent primitives, stores, MCP transport
├─ khonliang-bus-lib    — agent base/client for bus registration
└─ researcher-lib       — evaluation primitives

AGENTS/APPS
├─ researcher  — ingest world: papers, OSS, RSS → corpus
├─ developer   — dev lifecycle: FRs, specs, work units, git/PRs
├─ reviewer    — code review across models and vendors
└─ store       — artifact backend + viewer  ← THIS REPO
```

## Architecture boundary

- **khonliang-bus-lib** = library. Agent primitives. Don't reimplement.
- **khonliang-bus** = service. Store registers with it via bus-lib.
  Store does **not** run the bus itself.
- **khonliang-developer / khonliang-reviewer / khonliang-researcher**
  = sibling agents. No direct imports across these repos — any
  cross-agent interaction goes through the bus like every other
  agent-to-agent call.

When in doubt: if it's about *storing, reading, rendering, or
displaying an artifact*, it belongs here. The artifact lifecycle
(read, write, render, display) lives in this repo today.

## Phase roadmap

Each phase is its own FR. Do not stack them into a single PR — the
smaller-PR convention is what kept the scaffold separate from the
viewer skill.

1. **Phase 1** ✅ shipped — scaffold, health_check, tests, CLI
   (`fr_store_4ea7d48b`).
2. **Phase 2** ✅ shipped — artifact read skills (get, list,
   metadata, head, tail, grep, excerpt) (`fr_store_08c1c6b2`).
   Proxy to the bus artifact backend via
   `BusBackedArtifactStore`; the `ArtifactBackend` ABC is the
   swap point for Phase 4.
3. **Phase 3** ✅ shipped — viewer mode (`fr_store_d22556bb`).
   Browser URL for tabbed rendering. Graphviz, markdown, JSON
   tree, code highlighting; renderer registry extensible via
   `@register_renderer`.
4. **Phase 4a** ✅ shipped — `LocalArtifactStore` (SQLite) +
   `artifact_create` skill (`fr_store_73e5a6f4`). Config-gated
   default keeps the bus backend as the read source until 4b
   lands the union-read.
5. **Phase 4b** ✅ shipped — `CompositeArtifactBackend(local,
   bus)` (`fr_store_ef668d56`). Local-first reads with bus
   fallback; writes go local-only; `artifact_migrate_from_bus`
   skill copies bus-resident artifacts into local SQLite.
   `[artifacts] backend: composite` opts in.
6. **Phase 4c** ✅ shipped — bus PR #24 (2026-04-26). The bus's
   `bus_artifact_*` read tools were removed from the MCP adapter
   (only `bus_artifact_distill[_many]` remain); the bus REST
   surface is marked deprecated and slated for removal once all
   operators have run `artifact_migrate_from_bus`.
7. **Phase 5** ✅ shipped — researcher PR #34 (2026-04-26).
   `stage_payload` + `ingest_from_artifact` registered as bus
   skills on researcher-primary (`fr_researcher_000ad07c`),
   closing the write-side cross-reference between store (owns the
   bytes) and researcher (consumes them as corpus).

## Running

Preferred bus-native agent:

```bash
.venv/bin/python -m store.agent --id store-primary --bus http://localhost:8788 --config /abs/path/config.yaml
```

For dogfooding, start and restart store through khonliang-bus
lifecycle tools when the bus is running. Config paths must be
absolute for cross-session launches.

## MCP tool response convention

Same as researcher / developer / reviewer: token-efficient, no
preamble, data-only, default to brief.

## Claude's role

Pure code + code review. When this repo grows real functionality,
each addition goes through a PR with Copilot review; Claude-authored
commits get a cross-vendor review before merge (per the user-level
convention in `~/.claude/CLAUDE.md`).
