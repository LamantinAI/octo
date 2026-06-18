# config/ — example runtime configuration

A worked example of the on-disk runtime config layout from the `runtime_config.md`
and `petstore_case.md` vault drafts: a top-level `octo.toml` plus one folder per
dynamic connector under `connectors/`.

```
config/
├── octo.toml                       # main manifest (forward-looking, see below)
└── connectors/
    └── petstore/
        ├── petstore.toml           # one dyn HTTP connector = whole Petstore API
        └── models/                 # JSON-schema models, one file per schema
            ├── pet.json
            ├── category.json
            ├── tag.json
            └── pet_array.json
```

## What works today

The whole tree loads from `octo.toml` via the builder: register the connector
type's factory in code, then point the builder at the manifest.

```rust
let octo = Octo::builder()
    .register_connector_type("http", octo_connector_http::factory())
    .from_config_file("config/octo.toml")?   // scans [connectors], instantiates each
    .build();
```

`from_config_file` applies `[runtime]` (e.g. `bus_capacity`), builds a
`RuleBasedRouter` from the `[[router.routes]]` table, scans `[connectors] dir`
(folder-style `connectors/<id>/<id>.toml` and flat `connectors/<id>.toml`, plus
any `[[connectors.explicit]]`), resolves each `type =` to its registered
factory, and folds each connector's `register_payloads` into the bus registry.
Duplicate ids and unknown types are hard errors. An explicit `.router(...)` in
code wins over the table. See `tests/config_load.rs`.

A single connector can also be loaded directly without a manifest, via
`HttpConnector::from_file("config/connectors/petstore/petstore.toml")` — see
`tests/petstore_dyn.rs` (deterministic, local mock) and
`examples/petstore_dyn_round_trip.rs` (live API).

## What is forward-looking

`config_reload = "manual"` (load once at startup) is what runs today. **Hot
reload** (`"file_watch"` — a `notify`-based watcher emitting `octo.config.*`
envelopes, atomic mutations, agent `octo.config.*` tools) is the next
`runtime_config.md` step — i.e. editing this file currently requires a restart.
Runtime route mutations (`add_route`/`remove_route`/...) already exist on the
router; persisting them back to TOML is part of that same later step.
