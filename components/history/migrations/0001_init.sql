-- Per-channel conversation transcript. One row per turn, ordered by the
-- autoincrement id; the backend trims to its per-channel cap on append.
CREATE TABLE IF NOT EXISTS turns (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    channel    TEXT    NOT NULL,
    role       TEXT    NOT NULL,          -- "user" | "assistant"
    content    TEXT    NOT NULL,
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_turns_channel_id ON turns (channel, id);
