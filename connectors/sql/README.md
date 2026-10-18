# SQLite connector

`octo-connector-sql` provides up to 20 logical application stores in one dedicated SQLite file. It does not connect to the host's history or memory database and does not run a language model. The host supplies authenticated invocation and human-decision proofs from `octo-approval`.

Register `factory(authority, protected_paths)` using the same `Authority` as the host ingress/dispatch adapter. Without that native authority context, all commands fail closed. The factory performs no database initialization until its supervised connector starts.

```toml
[connector]
id = "sql"
type = "sql"
path = "./application.sqlite" # relative to this manifest
max_stores = 20               # cannot exceed 20; archived stores count
max_rows = 200
max_bytes = 262144
max_cell_bytes = 65536
max_affected_rows = 1000
max_pending = 32
timeout_ms = 5000
approval_ttl_seconds = 300

[connector.native]
max_sql_bytes = 65536
max_columns = 512
max_expression_depth = 64
max_trigger_depth = 32
max_parameters = 512
max_like_pattern_bytes = 1024
```

All limits have defaults. Native limits have validated SQLite/build upper bounds; attachment remains prohibited. SQLx and `libsqlite3-sys` are pinned together, with the bundled preupdate hook enabled.

On first startup, the connector automatically creates missing parent directories and the SQLite file. Newly created Unix directories use `0700` and the new file uses `0600`; existing directory/file permissions and data are preserved. Protected paths are checked before creation and after canonicalization, including symlinks and parent components.

The file must be separate from protected host files and directories, inaccessible to model shell/file tools, and writable by the service together with its journal/WAL sidecars. Canonical paths and Unix device/inode aliases are checked. A nonempty SQLite file without the connector's identity marker is rejected rather than imported. Versioned protected migrations preserve existing connector data and reject unsupported migration versions.

## Commands

Every request is a JSON object and rejects unknown fields. Store IDs, object IDs and column IDs are opaque. Obtain current physical names and revisions through `describe`; only that store's generated `s_<id>__` namespace is executable.

| Command | Arguments |
| --- | --- |
| `sql.store.create` | `name`, `purpose?`, `tags?`, `idempotency_key` |
| `sql.store.list`, `sql.store.search` | `query?`, `cursor?`, `limit?` (maximum 20) |
| `sql.store.describe` | `store_id`, `tables?` (physical names to restrict output) |
| `sql.store.update_metadata` | `store_id`, `expected_revision`, `metadata: [{object_id,column_id?,semantics}]` |
| `sql.store.archive` | `store_id`, `expected_revision`, `idempotency_key` |
| `sql.store.drop` | `store_id`, `expected_revision`, `idempotency_key` |
| `sql.execute` | `store_id`, `expected_revision`, `sql`, `parameters?`, `idempotency_key?`, `metadata?` |
| `sql.schema.apply` | `store_id`, `expected_revision`, `idempotency_key`, `table`, `columns`, `semantics?` |
| `sql.operation.status` | `operation_id`, `include_details?` |

`sql.schema.apply` creates a checked table in the selected namespace. Each column specifies `name`, `data_type` (`INTEGER`, `REAL`, `TEXT`, `BLOB`, `NUMERIC`), and optional `primary_key`, `nullable`, `semantics`. More complex schema definitions use `sql.execute`.

Parameters and result cells use tagged types: `{"type":"integer","value":1}`, `real`, `text`, `blob` (byte array), or `{"type":"null"}`. Writes and schema mutations require an idempotency key. Repeat the exact request with the same key to retrieve its persisted outcome; changed content with the same key is rejected. Each successful mutation advances the store revision. Metadata/schema deltas commit with user changes and the receipt.

Semantic metadata allows `description`, `units`, `null_meaning`, `enumeration`, `time_basis`, `timezone`, `provenance`, `relationships`. It is untrusted descriptive data. IDs persist for objects/columns whose physical names survive; renames create new identities and invalidate the old meanings. Out-of-band schema drift invalidates meanings and requires a fresh revision. Unregistered objects are not automatically adopted. Search returns descriptions, never row values.

## Supported SQL and approval

One complete top-level AST statement is required. SQLite trigger bodies may contain several internal statements. Supported native forms include SELECT/CTEs/joins/aggregates, INSERT, conflict INSERT/UPSERT/REPLACE, scoped UPDATE, DELETE, CREATE table/index/view/trigger, DROP objects, and ALTER TABLE ADD COLUMN, RENAME TABLE/COLUMN, DROP COLUMN. SQLite and parser errors remain explicit; unsupported ALTER variants, virtual tables/modules, arbitrary UDFs and unknown functions are rejected. The function allowlist lives in `engine/ast.rs`.

A scoped UPDATE with an equality predicate against values/parameters may run under the actual-effect limit. Broad or unclassified UPDATE, UPSERT, REPLACE, every DELETE, DROP and DROP COLUMN require one-time human confirmation. Native authorization rechecks trigger, view and foreign-key effects; hidden DELETE and indirect UPDATE cannot inherit approval from trigger creation. Stored conflict and update effects are classified conservatively.

`ATTACH`/`DETACH`, extensions, arbitrary files, model PRAGMAs, transaction control, global database maintenance, cross-store references and direct schema/control-table access are unavailable. AST validation and native authorization complement each other. User statements are freshly prepared under immutable operation policy; their prepared statements are never cached across policies.

Destructive requests return `approval_required`. The actor attaches a native approval presentation at the authenticated origin's private destination. Telegram shows a short action/resource card with Confirm, Cancel and Details buttons. Exact SQL/typed parameters, object names, limits, hash and physical identity are fetched privately with authenticated `sql.operation.status {operation_id,include_details:true}`; this neither approves the operation nor extends its lifetime. Display facts are captured with the immutable operation rather than generated by a language model. Console and Telegram retain `/sqlapprove <connector> <operation_id>` and `/sqldeny <connector> <operation_id>` as manual fallback. The internal `sql.approval.decide` event requires a separate native `DecisionProof`; it is not a model tool. JSON flags, guessed IDs, source names and approval text confer no authority. Policy/config changes invalidate pending approvals.

There are no automatic backups or undo guarantees. Committed destruction may be irreversible.

## Execution and recovery

`Database::open(path, limits, authority, protected_paths)` and `Database::handle(&Envelope)` expose the same backend for host integration tests. SQLx owns connections and transactions; native SQLite operations run in bounded blocking workers, serializing access to the connector connection. Actor admission and durable pending operations are bounded independently.

A consumed grant is durably claimed before user execution. User SQL, reconciled schema/metadata, receipt and final audit commit together. Native preupdate events count trigger/cascade/REPLACE changes; DROP TABLE deletions and DROP COLUMN rewrites are pre-counted under the same transaction, without double-counting their target rows. Limit violations and cancellation request rollback. Cancellation uses a native progress flag/deadline and never infers an outcome from dropping a future.

A lost response is recovered through the exact idempotency key or operation status. Persisted claimed work becomes `unknown` on reopening and is never automatically replayed. Commit errors are checked against receipts only after stopping/closing the old connection; an unprovable outcome remains `unknown`. Current read/store rights are checked again before returning cached results. Protected audit records include authenticated human decision actors and timestamps, while exact operation content stays in the protected journal.

Run the focused suite with `cargo +1.97.0 test -p octo-connector-sql`.
