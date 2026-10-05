-- NULL = recorded but handler has not finished yet (so it must be retried).
ALTER TABLE events ADD COLUMN IF NOT EXISTS handled_at TIMESTAMPTZ;
