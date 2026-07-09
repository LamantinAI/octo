# 🐙 octo

`octo` is an **event-driven runtime for embodied, always-on agents** — a
distributed nervous system inspired by the octopus: many autonomous *tentacles*
(connectors) sensing and acting on the world, a *core* that routes events, fast
*reflexes*, and *cognition* only when it's actually needed.

> Not a brain, but a system that lives in time and reacts to the world.

Octo is **not an agent** — it is the *environment in which an agent exists and
behaves*. It owns the part that LLM SDKs, graph frameworks, and tool protocols
leave out: the continuous, supervised, multi-sensor runtime that runs forever and
stays consistent over time.

Built in Rust on tokio.

## Overview

A running Octo is a supervised tree of long-lived **connectors** publishing and
consuming **envelopes** on an in-process **bus**. Two kinds of actor consume that
stream:

- **Reflex** (`Router`) — fast, deterministic, no LLM. Handles the majority of cases.
- **Cognition** (`Cogitator`) — LLM / workflow reasoning, for ambiguity and complex decisions. Pluggable: you bring the brain.

```
event in → normalize → reflex (deterministic) → ↳ escalate to cognition (if unsure) → action(s) out → memory
```

Connectors are also the agent's **action space**: cognition reaches the world by
emitting envelopes back to connectors (env-as-tools). The same envelope shape
flows in both directions — only the header differs.

## The envelope

[`Envelope`] is a fixed-shape, HTTP/NATS-style header (id, source, target, kind,
channel, timestamp, trail, …) carrying an opaque [`Payload`]. The bus routes by
header fields; handlers downcast the payload to a known type. Kinds are
dot-namespaced with glob matching (`vision.**`, `mqtt.factory.*`). An optional
`PayloadRegistry` enforces the type expected for a kind at publish time.

## Features

- **Connectors as autonomous, supervised tasks** — each owns its own lifecycle
  (reconnect, retry, restart, graceful shutdown) and runs on its own cadence, not
  a central request loop. Input-only, output-only, or bidirectional.
- **In-process bus** with declarative `Filter`s (by kind glob, source, target,
  channel, correlation) and a broker-style request/response
  (`publish_and_await_response`).
- **Typed payloads** — `PayloadRegistry` validates that a kind carries its
  expected type; mismatches are rejected before they reach subscribers.
- **Pluggable cognition** — implement the `Cogitator` trait; the runtime
  pre-subscribes it so it observes every matching envelope. Perception scope is
  the cogitator's filter (narrow inbox → whole bus).
- **Rule-based reflex routing** — `RuleBasedRouter` rewrites/targets envelopes
  deterministically (e.g. `vision.incident.* → alerter`), recording its action in
  the envelope's trail.
- **Supervision + control-plane** — per-connector `RestartPolicy` with backoff and
  panic isolation, plus `octo.control.restart_connector` / `restart_process`
  signals so an inhabitant can **restart a connector or the whole process by
  emitting an envelope** (self-restart after applying config).
- **Media & streaming** — `Blob` (bytes + MIME) payloads; chunked streams
  (`Open`/`Chunk`/`Close`) collected by correlation id.
- **Observable** — every reflex/cognition decision is appended to the envelope's
  `trail`.

## Architecture notes

- **Tentacles vs. request-driven services.** A connector runs continuously and
  pushes events on its own schedule — distinct from Tower/axum-style handlers that
  fire only on delivery. Octo's value is exactly this connector lifecycle +
  supervision layer; downstream dispatch may still borrow request-driven idioms.
- **Core stays general.** `octo-core` is protocol/transport only — connectors,
  channels, envelopes, bus, lifecycle FSM, runtime builder. Behavioral actors
  (cognition) and integrations live in sibling crates, so the kernel never assumes
  a chat- or vision-shaped world.
- **Envelope is NATS-shaped on purpose** — the in-process bus can later back onto
  a distributed broker without changing connector code.
- **Cognition is userland.** The `Cogitator` is a single pluggable trait; the
  runtime supervises it like any actor but holds no opinion about how it thinks.

## Layout

```
octo/                       ← workspace root
├── octo-core/              ← the kernel: bus, envelope, connectors, lifecycle, router, runtime
├── components/             ← pluggable, backend-swappable libraries (not the kernel, not connectors)
│   ├── history/            ← per-channel conversation history (in-memory / file backends)
│   └── http-auth/          ← reusable auth modes for connectors (basic / bearer / oauth2)
├── octo-rig/               ← a rig Tool bridging native LLM tool-calling → connector dispatch
├── connectors/
│   ├── http/               ← dynamic, TOML-configured HTTP connector (one crate, many APIs)
│   ├── petstore/           ← example HTTP connector instance
│   ├── telegram/           ← bidirectional Telegram connector (teloxide)
│   ├── scheduler/          ← manageable-actor scheduler (control commands mutate persisted state)
│   └── caldav/             ← generic CalDAV calendar connector (one crate, many calendars)
└── octolab/                ← playground: a real ReAct LLM agent over the runtime
```

## A taste

```rust
use std::sync::Arc;
use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId,
    Envelope, EventKind, Octo, OctoResult,
};

struct Heartbeat { id: ConnectorId, caps: ConnectorCapabilities }

#[async_trait]
impl Connector for Heartbeat {
    fn id(&self) -> &ConnectorId { &self.id }
    fn capabilities(&self) -> &ConnectorCapabilities { &self.caps }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        ctx.publish(Envelope::new(self.id.clone(), EventKind::from_static("tick"), 1u64)).await
    }
}

#[tokio::main]
async fn main() -> OctoResult<()> {
    let hb = Arc::new(Heartbeat {
        id: ConnectorId::new("heartbeat"),
        caps: ConnectorCapabilities::input_only(),
    });
    // Add a Cogitator and connectors; the runtime supervises them all.
    Octo::builder().add_connector(hb).build().run().await
}
```

## octolab

`octolab/` is the playground that exercises the runtime as a real online agent:
a rig + OpenRouter ReAct cogitator with native tool-calling, the official Telegram
connector (console fallback), env-as-tools dispatch to an HTTP connector, modular
per-channel history, and configurable perception. It is excluded from
`default-members`, so a plain `cargo test` stays light:

```sh
cargo test                 # kernel + light crates
cargo run -p octolab       # the agent (needs OCTO_* env / repo-root .env)
```

## Status

Working and covered by tests: the envelope/bus/filter core, typed payload
registry, request/response, connector lifecycle FSM, rule-based reflex routing,
supervision with restart policies, the control-plane self-restart signals,
streaming frames, and the pluggable cogitator pipeline. Rough edges:

- **Bus backpressure.** The in-process broadcast silently drops on lag — a slow
  cognition layer under a burst can miss envelopes. Per-subscriber backpressure
  isn't wired yet.
- **Inbound media.** The outbound media path (`Blob` → connector) works; the
  inbound half (e.g. a photo *into* an envelope) is not built.
- **Single-process bus.** `InProcessBus` only; the NATS/broker-backed distributed
  bus is designed-for but not implemented.

## Position in the ecosystem

| Layer | Role |
|-------|------|
| LLM SDKs | reasoning |
| Graph frameworks | structured workflows |
| MCP / tools | external capabilities |
| **Octo** | **the continuous runtime** |

Octo owns the bottom row: the always-on, supervised substrate the reasoning,
workflow, and tool layers plug into.

## License

MIT — see [LICENSE](LICENSE).
