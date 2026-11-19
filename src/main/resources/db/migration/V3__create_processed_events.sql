-- Tabel inbox untuk consumer idempotent: satu baris per event yang sudah diproses
CREATE TABLE processed_events
(
    event_id     VARCHAR(36)  NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    processed_at DATETIME(6)  NOT NULL,
    CONSTRAINT pk_processed_events PRIMARY KEY (event_id)
);
