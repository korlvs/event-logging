CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id TEXT NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL,
    category TEXT NOT NULL,
    action TEXT NOT NULL,
    operation_type INT NOT NULL,
    status INT NOT NULL,
    actor_id TEXT,
    actor_type TEXT,
    actor_display_name TEXT,
    client_ip TEXT,
    correlation_id TEXT,
    source_service TEXT,
    environment TEXT,
    user_agent TEXT,
    resource_id TEXT,
    resource_type TEXT,
    resource_details JSONB,
    details JSONB,
    schema_version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_events_timestamp ON audit_events(timestamp);
CREATE INDEX idx_audit_events_category ON audit_events(category);
CREATE INDEX idx_audit_events_action ON audit_events(action);
CREATE INDEX idx_audit_events_operation_type ON audit_events(operation_type);
CREATE INDEX idx_audit_events_status ON audit_events(status);
CREATE INDEX idx_audit_events_actor_id ON audit_events(actor_id);
CREATE INDEX idx_audit_events_correlation_id ON audit_events(correlation_id);
CREATE INDEX idx_audit_events_source_service ON audit_events(source_service);
CREATE INDEX idx_audit_events_resource_id ON audit_events(resource_id);