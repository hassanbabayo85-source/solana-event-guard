-- Last processed position per stream (one stream = one watched address/program).
CREATE TABLE IF NOT EXISTS checkpoints (
    stream_id       TEXT PRIMARY KEY,
    last_signature  TEXT        NOT NULL,
    last_slot       BIGINT      NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every event we have seen. The UNIQUE key is the dedupe mechanism:
-- INSERT ... ON CONFLICT DO NOTHING tells us whether an event is new.
CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,
    stream_id    TEXT        NOT NULL,
    signature    TEXT        NOT NULL,
    ix_index     INTEGER     NOT NULL DEFAULT 0,
    slot         BIGINT      NOT NULL,
    payload      JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, signature, ix_index)
);
CREATE INDEX IF NOT EXISTS events_stream_slot_idx ON events (stream_id, slot);

-- Delivery log: one row per delivery attempt. Basis for retry and replay.
CREATE TABLE IF NOT EXISTS deliveries (
    id             BIGSERIAL PRIMARY KEY,
    event_id       BIGINT      NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    attempt        INTEGER     NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    response_code  INTEGER,
    error          TEXT,
    next_retry_at  TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS deliveries_retry_idx
    ON deliveries (next_retry_at) WHERE status <> 'delivered';
