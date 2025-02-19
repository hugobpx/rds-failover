-- Add migration script here
CREATE TABLE IF NOT EXISTS counter (
    id BIGSERIAL PRIMARY KEY,
    value BIGINT NOT NULL
);
