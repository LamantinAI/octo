CREATE TABLE history_sessions (
    catalog_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,
    access_kind TEXT NOT NULL CHECK(access_kind IN ('private','group_bound')),
    owner_key TEXT,
    connector_key TEXT,
    channel_key TEXT,
    host TEXT NOT NULL,
    title TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('permanent','temporary')),
    state TEXT NOT NULL CHECK(state IN ('active','archived')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
    creation_request TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK((access_kind = 'private' AND owner_key IS NOT NULL AND connector_key IS NULL AND channel_key IS NULL)
       OR (access_kind = 'group_bound' AND owner_key IS NULL AND connector_key IS NOT NULL AND channel_key IS NOT NULL AND kind = 'permanent'))
);
CREATE UNIQUE INDEX history_sessions_group ON history_sessions(connector_key,channel_key)
    WHERE access_kind = 'group_bound';
CREATE INDEX history_sessions_owner ON history_sessions(owner_key,catalog_id);
CREATE TABLE history_session_journals (
    session_id TEXT NOT NULL REFERENCES history_sessions(session_id),
    run_id TEXT NOT NULL,
    history_key TEXT NOT NULL UNIQUE,
    PRIMARY KEY(session_id,run_id)
);
CREATE TABLE history_session_defaults (
    connector_key TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES history_sessions(session_id),
    PRIMARY KEY(connector_key,channel_key)
);
