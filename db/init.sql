CREATE TABLE IF NOT EXISTS transactions (
    txn_id          TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    user_id         TEXT NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  TEXT NOT NULL,
    topic         TEXT NOT NULL,
    msg_key       TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS decisions (
    txn_id         TEXT PRIMARY KEY,
    user_id        TEXT NOT NULL,
    amount         NUMERIC(14, 2) NOT NULL,
    city           TEXT NOT NULL,
    payee_id       TEXT NOT NULL,
    payee_type     TEXT NOT NULL,
    category       TEXT NOT NULL,
    device_id      TEXT NOT NULL,
    decision       TEXT NOT NULL,
    risk_score     DOUBLE PRECISION NOT NULL,
    rule_score     DOUBLE PRECISION NOT NULL,
    ml_score       DOUBLE PRECISION NOT NULL,
    anomaly_score  DOUBLE PRECISION NOT NULL,
    rule_hits      JSONB NOT NULL,
    reasons        JSONB NOT NULL,
    features       JSONB NOT NULL,
    ground_truth   TEXT,
    model_version  TEXT NOT NULL,
    latency_ms     DOUBLE PRECISION NOT NULL,
    event_ts       BIGINT NOT NULL,
    decided_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status         TEXT NOT NULL DEFAULT 'OPEN',
    label          TEXT,
    labeled_by     TEXT,
    labeled_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_decisions_time ON decisions (decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_decisions_alerts ON decisions (decided_at DESC) WHERE decision <> 'APPROVE';
CREATE INDEX IF NOT EXISTS idx_decisions_labeled ON decisions (labeled_at) WHERE label IS NOT NULL;
