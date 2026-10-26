-- Public journal IDs are scoped to a session journal, independent from SQLite PKs.
CREATE TABLE history_public_turns (
    history_key TEXT NOT NULL REFERENCES history_session_journals(history_key),
    public_id INTEGER NOT NULL CHECK(public_id > 0),
    turn_id INTEGER NOT NULL UNIQUE REFERENCES turns(id),
    PRIMARY KEY(history_key,public_id)
);
CREATE TABLE history_public_compacts (
    history_key TEXT NOT NULL REFERENCES history_session_journals(history_key),
    public_id INTEGER NOT NULL CHECK(public_id > 0),
    compact_id INTEGER NOT NULL UNIQUE REFERENCES compacts(id),
    PRIMARY KEY(history_key,public_id)
);
-- Existing authoritative rows keep every ID already exposed before this migration.
INSERT INTO history_public_turns(history_key,public_id,turn_id)
    SELECT j.history_key,t.id,t.id FROM history_session_journals j JOIN turns t ON t.channel=j.history_key;
INSERT INTO history_public_compacts(history_key,public_id,compact_id)
    SELECT j.history_key,c.id,c.id FROM history_session_journals j JOIN compacts c ON c.channel=j.history_key;
CREATE TABLE history_session_promotions (
    session_id TEXT PRIMARY KEY REFERENCES history_sessions(session_id),
    promotion_key TEXT NOT NULL UNIQUE,
    fingerprint TEXT NOT NULL,
    source_revision INTEGER NOT NULL,
    target_revision INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT(datetime('now'))
);
CREATE INDEX journal_appends_request_lookup ON journal_appends(namespace,idempotency_key,channel);
