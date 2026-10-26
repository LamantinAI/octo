-- Strict retained-journal appends. Old capped constructors keep their original APIs.
CREATE TABLE journal_appends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    namespace TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    batch_digest TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(channel, namespace, idempotency_key)
);
CREATE TABLE journal_append_records (
    append_id INTEGER NOT NULL REFERENCES journal_appends(id),
    ordinal INTEGER NOT NULL,
    turn_id INTEGER NOT NULL REFERENCES turns(id),
    PRIMARY KEY(append_id, ordinal),
    UNIQUE(turn_id)
);
-- Completed remaps make retries inspectable and prevent old writers resurrecting a source.
CREATE TABLE journal_key_remaps (
    source_key TEXT PRIMARY KEY,
    target_key TEXT NOT NULL UNIQUE,
    inventory TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
