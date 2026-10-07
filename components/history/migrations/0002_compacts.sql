-- A checkpoint covers a committed prefix of one channel's retained messages.
-- Original messages and older checkpoints are never deleted by compaction.
CREATE TABLE IF NOT EXISTS compacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    through_id INTEGER NOT NULL REFERENCES turns(id),
    previous_id INTEGER REFERENCES compacts(id),
    content TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_compacts_channel_id ON compacts(channel, id);
