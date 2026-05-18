CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    published_time TIMESTAMPTZ NOT NULL,
    initiator TEXT NOT NULL,
    state_before TEXT,
    state_after TEXT,
    change_tag TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_events_source_system ON events(source_system);
CREATE INDEX idx_events_change_tag ON events(change_tag);
CREATE INDEX idx_events_event_time ON events(event_time);
CREATE INDEX idx_events_published_time ON events(published_time);