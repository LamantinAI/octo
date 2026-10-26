-- Alias receipts share an accepted batch's turns; they never duplicate those turns.
CREATE TABLE journal_append_records_v2 (
    append_id INTEGER NOT NULL REFERENCES journal_appends(id),
    ordinal INTEGER NOT NULL,
    turn_id INTEGER NOT NULL REFERENCES turns(id),
    PRIMARY KEY(append_id,ordinal)
);
INSERT INTO journal_append_records_v2 SELECT append_id,ordinal,turn_id FROM journal_append_records;
DROP TABLE journal_append_records;
ALTER TABLE journal_append_records_v2 RENAME TO journal_append_records;
CREATE INDEX journal_append_records_turn ON journal_append_records(turn_id);
