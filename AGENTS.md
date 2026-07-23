# AGENTS.md — octo

Instructions for an AI agent (or human) working in this repository. Read this
before changing code.

## Project Summary

`octo` is an **event-driven runtime for embodied, always-on agents** — a
distributed-nervous-system model (octopus): many autonomous *tentacles*
(connectors) sensing and acting on the world, a *core* that routes typed
envelopes on an in-process bus, fast *reflexes* (the `Router`), and *cognition*
(a pluggable `Cogitator`) only when it's actually needed.

octo is **not an agent** — it is the *environment in which an agent exists and
behaves*. It owns the part LLM SDKs, graph frameworks, and tool protocols leave
out: the continuous, supervised, multi-sensor runtime. Rust on tokio; workspace
edition 2024, toolchain 1.85+; shared deps in the root `Cargo.toml`
`[workspace.dependencies]`.

Workspace members:

- `octo-core/` — the kernel: envelope, bus, connectors + lifecycle, router,
  runtime builder, the `Cogitator` trait, control-plane. **Protocol/transport
  only** — no behavioral actors.
- `components/history/` — `octo-history`: pluggable per-channel conversation
  transcript (in-memory / file backends), LLM-agnostic.
- `components/http-auth/` — `octo-http-auth`: reusable auth modes for connectors
  (basic / bearer / oauth2 / none), secrets from named env vars.
- `connectors/*` — the organs: `telegram` (bidir, edge ACL, factory), `scheduler`
  (alarms), `caldav` (generic CalDAV calendar), `http` (dyn TOML-configured), and
  `petstore` (example instance).
- `octo-rig/` — a `rig` `Tool` bridging native tool-calling to connector dispatch
  (`dispatch_to_connector`: publish a command, await the correlated result), plus
  typed tools (file tools behind `code`, `SendFileTool`, `RestartTool`).

Design detail lives in the research vault (`research/`, a separate git repo) and
the `README.md`.

## How To Work In This Repository

Orient by crate and responsibility first:

1. **The kernel is `octo-core`, and it stays general.** Envelope, bus, connector
   trait + lifecycle FSM, router, runtime, the `Cogitator` trait — protocol and
   transport only. It must never assume a chat- or vision-shaped world. Behavioral
   actors (cognition) and integrations live in sibling crates.
2. **Cognition is userland.** The `Cogitator` is a single pluggable trait; the
   runtime supervises it like any actor but holds no opinion about how it thinks.
   `EmptyCogitator` is the no-op default. Do not bake a specific cognition model
   into core.
3. **A connector is an autonomous supervised task** implementing `Connector`
   (`id`, `capabilities`, `run`, `restart_policy`, `register_payloads`). It owns
   its lifecycle and pushes events on its own cadence. Config-driven connectors
   also provide a `ConnectorFactory` (`type = "..."`) loaded via
   `from_config_file`. Follow the shape of `connectors/scheduler` and
   `connectors/telegram`.
4. **Shared deps** live in the root `Cargo.toml` under `[workspace.dependencies]`;
   pull them into a crate with `dep.workspace = true`. Reusable connector helpers
   go in `components/`, not duplicated per connector.
5. **`octo-core` is the source of truth for shared types** — connectors and
   adapters consume them; do not duplicate.

## Local Runbook

- `cargo test` — kernel + light crates (`default-members` deliberately excludes
  `octo-rig` / `telegram`, which pull the rig / teloxide trees).
- `cargo test --workspace` — everything, including the heavy crates.

## Module Organisation

Keep code **modular, grouped by purpose**, one concern per file. **Cap each file at
~500 lines (hard limit 600).** `octo-core` already uses a `mod.rs`-with-submodules
layout where a concern grew past a flat file:

```
octo-core/src/
├── lib.rs
├── runtime.rs              ← Octo + OctoBuilder: assemble bus, cogitator, router, connectors; from_config_file
├── bus.rs                  ← InProcessBus, Filter, Subscription (+ per-subscriber backpressure shim)
├── config.rs               ← manifest loading (octo.toml + connectors dir), ConnectorFactory registry
├── control.rs              ← control-plane kinds (octo.control.restart_*)
├── error.rs  ids.rs
├── cogitator/              ← the Cogitator trait + CogitatorContext + EmptyCogitator
├── connector/              ← Connector trait, capabilities; channel, lifecycle (FSM/restart), subscription (SubscribeOptions/backpressure)
├── envelope/               ← Envelope, Payload, EventKind (glob), metadata (trust), registry, stream frames, trail, blob
└── router/                 ← Router trait, Route predicates, RuleBasedRouter
```

Split a flat file into `mod.rs`-with-submodules when it passes ~500 lines or more
than a few cohesive concerns accumulate. Shared cross-submodule types live in the
parent `mod.rs`.

## Import Rule (strict)

- **Import the final entity, in full, by name** — structs, enums, functions,
  traits, constants. `Duration::from_secs(..)`, not `std::time::Duration::from_secs`;
  `info!(..)`, not `tracing::info!`.
- **Group items from the same module/crate path in braces**: one
  `use std::{sync::Arc, time::Duration};`, one `use crate::bus::{EventBus, Filter, InProcessBus};`
  — never one `use` line per item from the same path.
- **No module paths in code bodies.** Import the leaf; don't `use module` and then
  write `module::Type::method` throughout. Associated fns on an imported type are
  fine (the type is imported).
- **No glob imports** in implementation code.
- **On name collisions, alias** the leaf, or use a leading `::` to disambiguate an
  extern crate from a same-named local module.
- **Group blocks**: std, then third-party crates, then `crate::` — blank line
  between groups.
- Reasonable exemption: attribute macros stay pathed (`#[tokio::main]`).

In short: explicit, brace-grouped imports; no long module paths in the code body.

## Conventions

- **No emoji in code or logs.** Arrows / quotes / bullets are fine.
- **No `anyhow`.** Errors are explicit — `octo-core` defines `OctoError`
  (`thiserror` enum) and `OctoResult<T>`; add a variant for a new failure mode
  rather than stuffing context into an existing one. Connectors stringify foreign
  errors at their boundary, not `octo-core`.
- **Commit style**: Conventional-Commits-style lowercase prefixes — `feature:`,
  `fix:`, `chore:`, `docs:`, `git:` (note `feature`, not `feat`). No
  `Co-Authored-By` trailer.
- **Everything is an event.** Connectors emit/accept envelopes; reflexes and the
  cogitator react. Every reflex/cognition decision is appended to the envelope's
  `trail` (observability). Config-driven actors (router, dyn connectors,
  scheduler) keep their state as data (TOML/JSON), mutated via `octo.*.*` control
  envelopes — the manageable-actor pattern.

## Architecture Notes

- **Tentacles vs. request-driven services.** A connector runs continuously and
  pushes on its own schedule — distinct from Tower/axum handlers that fire only on
  delivery. octo's value is the connector lifecycle + supervision layer; downstream
  dispatch may still borrow request-driven idioms.
- **Envelope is NATS-shaped on purpose** — the in-process bus can later back onto a
  distributed broker without changing connector code.
- **Backpressure is per-subscriber** (`SubscribeOptions` / `BackpressureStrategy`:
  DropOldest / DropNewest / Throttle / `Steer` / best-effort Block). `Steer`
  supersedes by channel/correlation — the steering primitive. True global `Block`
  is intentionally not offered (incompatible with a fan-out broadcast).

## Out Of Scope

- **Distributed / NATS-backed bus** — the envelope is shaped for it, but only the
  in-process bus is implemented. Not a current feature.
- **A specific cognition engine in core** — cognition is userland; core supervises
  a `Cogitator` trait and stays agnostic.
