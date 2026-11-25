CREATE TABLE history_session_forks (
    session_id TEXT PRIMARY KEY REFERENCES history_sessions(session_id) ON DELETE CASCADE,
    parent_session_id TEXT NOT NULL REFERENCES history_sessions(session_id) ON DELETE RESTRICT,
    parent_through_id INTEGER NOT NULL CHECK(parent_through_id >= 0),
    parent_compact_id INTEGER REFERENCES compacts(id) ON DELETE RESTRICT,
    CHECK(session_id <> parent_session_id)
);
CREATE INDEX history_session_forks_parent ON history_session_forks(parent_session_id);
