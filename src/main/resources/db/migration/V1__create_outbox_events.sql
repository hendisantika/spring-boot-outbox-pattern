CREATE TABLE outbox_events
(
    id             VARCHAR(36)  NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id   VARCHAR(100) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    payload        JSON         NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_error     VARCHAR(1000),
    created_at     DATETIME(6)  NOT NULL,
    sent_at        DATETIME(6),
    CONSTRAINT pk_outbox_events PRIMARY KEY (id)
);

-- Relay mengambil event PENDING urut created_at
CREATE INDEX idx_outbox_events_status_created_at ON outbox_events (status, created_at);
-- Cleanup event SENT yang sudah lama
CREATE INDEX idx_outbox_events_status_sent_at ON outbox_events (status, sent_at);
