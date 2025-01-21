-- Your SQL goes here
ALTER TABLE chunks
    ADD COLUMN parity_shard BOOLEAN NOT NULL DEFAULT FALSE;