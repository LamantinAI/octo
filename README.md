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

## Connectors — env-as-tools organs

A connector is a long-lived, supervised task that senses and/or acts on one slice
of the world (a chat, a calendar, a mailbox, a shell). A **bidirectional**
connector is also a *capability the agent can invoke*: it advertises a catalog,
accepts **command** envelopes, and answers with a correlated **result**. The
cognition layer's entire action space is the set of registered connectors — add a
connector and the agent gains a skill, with **zero cognition change**. That's
"env-as-tools".

Because the contract is generic, most connectors are **one crate → many
instances**: the same code becomes many skills via config (many calendars, many
mailboxes, a swappable storage backend). And for HTTP APIs there's often **no
code at all** — the generic `http` connector builds a whole multi-route API from
a manifest (see [Configurable connectors](#configurable-connectors--a-whole-api-from-a-manifest)).

So there are **two ways to add a capability**:
- **A native connector** — a Rust crate (caldav / mail / storage / telegram …)
  when the integration needs real logic, a protocol client, or side effects.
- **A configurable connector** — just a manifest, no code, when the integration
  is "call this REST API" (the `http` connector).

### The dispatch contract (the API)

A command is an envelope addressed at a connector's `id`; the reply is an
envelope of kind `<command>.result` carrying the command's `id` as its
`correlation_id`:

```
command:  Envelope { target = "<connector-id>", kind = "<verb>", payload = <JSON> }
result:   Envelope { kind = "<verb>.result", correlation_id = <command.id>, payload = <JSON> }
```

- **Request/response** in one call: `bus.publish_and_await_response(cmd, timeout)`.
- **Errors are returned as data** (`{ "error": "..." }`), not as failures — so a
  reasoning layer reads the problem and adapts instead of crashing.
- **The catalog** each connector sets with `ConnectorCapabilities::with_description(..)`
  is what tells a model (or a human) which verbs it accepts and their payloads.

From an LLM, [`octo-rig`]'s `OctoDispatchTool` exposes *every* registered
connector to a rig agent as one tool (`dispatch_to_connector { target, kind,
payload }`), with the catalogs concatenated into its description. (octo-rig also
ships typed tools: the octo-code file tools behind its `code` feature, plus
`SendFileTool` and `RestartTool`.)

### Config-driven assembly (many instances, no new code)

Register a connector *type* once, then every manifest of that type becomes an
instance:

```rust
let octo = Octo::builder()
    .register_connector_type("caldav", octo_connector_caldav::factory())
    .register_connector_type("mail",   octo_connector_mail::factory())
    .from_config_file("octo.toml")?      // scans [connectors] dir for <id>.toml files
    .build();
```

```toml
# octo.toml
[connectors]
dir = "config/connectors"                # each *.toml here is one connector instance

# config/connectors/calendar-work.toml
[connector]
id       = "calendar-work"               # unique — this is how the agent addresses it
type     = "caldav"                      # picks the factory
base_url = "https://caldav.yandex.ru"
auth     = "basic"
login    = "work@example.com"
password_env = "OCTO_WORK_APP_PASSWORD"  # secrets are named env vars, never literals
```

Two work + personal calendars, or three mailboxes, are just N manifest files with
distinct `id`s — the model sees N distinct tools.

### Configurable connectors — a whole API from a manifest

Some capabilities need **no Rust at all**. The **`http` connector** is generic:
one crate that turns a declarative `type = "http"` manifest into an env-as-tools
organ. Each `[[connector.endpoint]]` maps a command kind → one HTTP call → a
correlated response kind, with path/query params pulled from the payload by a
JSONPath subset, plus optional JSON-schema models, header auth, timeout and
retry. It's the sweet spot between OpenClaw-style skills and MCP servers:
**integrate a REST API by writing config, not code.**

```toml
[connector]
id       = "petstore"
type     = "http"
base_url = "https://petstore.example.com"

[[connector.endpoint]]
cmd_kind     = "petstore.cmd.find_pets_by_status"  # the command the model dispatches
method       = "GET"
path         = "/pet/findByStatus"
query_params = { status = "$.status" }             # pulled from the command payload
response_kind = "petstore.event.pets_found"        # the correlated result

[[connector.endpoint]]
cmd_kind      = "petstore.cmd.delete_pet"
method        = "DELETE"
path          = "/pet/{petId}"
path_params   = { petId = "$.id" }                 # {petId} filled from the payload
response_kind = "petstore.event.pet_deleted"
```

Register the `http` factory once and every such manifest becomes a full
multi-route API the agent can call — so a new integration is often a manifest,
not a crate. `connectors/petstore` is a worked example.

### Shipped connectors

| id / type | what it is | key commands |
|-----------|------------|--------------|
| `http`      | **configurable** — a whole REST API from a manifest, no code (many APIs) | endpoints declared per-manifest |
| `caldav`    | CalDAV calendars (many calendars) | `calendar.list_events` / `create_event` / `delete_event` |
| `mail`      | IMAP read + SMTP send (one mailbox) | `mail.cmd.list` / `read` / `send` / `reply` |
| `storage`   | durable object store (local now, S3-ready) | `storage.put` / `get` / `list` / `delete` / `promote` / `checkout` |
| `telegram`  | bidirectional chat (teloxide) + file transfer + per-chat ACL | in: `chat.message`; out: `chat.reply` / `chat.send_file` |
| `scheduler` | reminders / alarms (manageable actor) | control commands mutate persisted state |
| `forkd`     | sandboxed script execution (executable skills) | `forkd.run` |

### Writing one

A connector is two traits:

- **`Connector`** — `id()`, `capabilities()`, and an async `run(self, ctx)` loop
  that subscribes to its commands (`Filter::by_target(self.id)`), does the work,
  and publishes each `<kind>.result` correlated to the request. Return errors as
  data.
- **`ConnectorFactory`** — `type_name()` and `create(id, manifest, ctx)`, building
  one instance from a `[connector]` table (secrets pulled from the env vars it
  names).

`connectors/caldav`, `connectors/mail` and `connectors/storage` are compact,
idiomatic templates — copy their shape. A native capability is a new crate; the
agent picks it up the moment its factory is registered and a manifest exists. (For
a plain REST API, don't write a crate — use the configurable `http` connector
above.)

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
