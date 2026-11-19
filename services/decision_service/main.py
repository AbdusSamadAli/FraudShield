import logging
import threading

import psycopg
from psycopg.types.json import Jsonb

from common.config import Settings, get_settings
from common.decision import APPROVE, decide, risk_score
from common.kafka import now_ms
from common.logging_setup import setup_logging
from common.metrics import DECISIONS, DUPLICATES, E2E_LATENCY, FEEDBACK_APPLIED
from common.processor import Output, StreamProcessor, run_processors
from common.topics import ALERTS, DECISIONS as DECISIONS_TOPIC
from common.topics import FEEDBACK, SCORED

log = logging.getLogger("decision-service")

SERVICE = "decision-service"
FEEDBACK_SERVICE = "decision-feedback"
VALID_LABELS = {"FRAUD", "LEGIT"}

INSERT_DECISION = """
INSERT INTO decisions (
    txn_id, user_id, amount, city, payee_id, payee_type, category, device_id, decision,
    risk_score, rule_score, ml_score, anomaly_score, rule_hits, reasons, features,
    ground_truth, model_version, latency_ms, event_ts, status
) VALUES (
    %(txn_id)s, %(user_id)s, %(amount)s, %(city)s, %(payee_id)s, %(payee_type)s, %(category)s,
    %(device_id)s, %(decision)s, %(risk_score)s, %(rule_score)s, %(ml_score)s, %(anomaly_score)s,
    %(rule_hits)s, %(reasons)s, %(features)s, %(ground_truth)s, %(model_version)s, %(latency_ms)s,
    %(event_ts)s, %(status)s
)
ON CONFLICT (txn_id) DO NOTHING
RETURNING txn_id
"""

APPLY_LABEL = """
UPDATE decisions
SET label = %s, labeled_by = %s, labeled_at = to_timestamp(%s / 1000.0), status = 'RESOLVED'
WHERE txn_id = %s
"""


class Database:
    def __init__(self, url: str):
        self.url = url
        self.lock = threading.Lock()
        self.conn = psycopg.connect(url, autocommit=True)

    def execute(self, sql: str, params) -> list:
        with self.lock:
            try:
                cur = self.conn.execute(sql, params)
            except psycopg.OperationalError:
                log.warning("reconnecting to database")
                self.conn = psycopg.connect(self.url, autocommit=True)
                cur = self.conn.execute(sql, params)
            return cur.fetchall() if cur.description else []


class DecisionHandler:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.review = settings.review_threshold
        self.block = settings.block_threshold

    def handle(self, event: dict) -> list[Output]:
        txn = event["transaction"]
        risk = risk_score(event["rule_score"], event["ml_score"], event["anomaly_score"])
        decision = decide(risk, self.review, self.block)
        latency_ms = float(max(now_ms() - txn["timestamp"], 0))
        record = {
            "transaction": txn,
            "decision": decision,
            "risk_score": risk,
            "rule_score": event["rule_score"],
            "ml_score": event["ml_score"],
            "anomaly_score": event["anomaly_score"],
            "rule_hits": event["rule_hits"],
            "reasons": event["reasons"],
            "features": event["features"],
            "model_version": event["model_version"],
            "latency_ms": latency_ms,
            "decided_at": now_ms(),
        }
        inserted = self.db.execute(
            INSERT_DECISION,
            {
                "txn_id": txn["txn_id"],
                "user_id": txn["user_id"],
                "amount": txn["amount"],
                "city": txn["city"],
                "payee_id": txn["payee_id"],
                "payee_type": txn["payee_type"],
                "category": txn["merchant_category"],
                "device_id": txn["device_id"],
                "decision": decision,
                "risk_score": risk,
                "rule_score": event["rule_score"],
                "ml_score": event["ml_score"],
                "anomaly_score": event["anomaly_score"],
                "rule_hits": Jsonb(event["rule_hits"]),
                "reasons": Jsonb(event["reasons"]),
                "features": Jsonb(event["features"]),
                "ground_truth": txn.get("ground_truth"),
                "model_version": event["model_version"],
                "latency_ms": latency_ms,
                "event_ts": txn["timestamp"],
                "status": "CLOSED" if decision == APPROVE else "OPEN",
            },
        )
        if inserted:
            DECISIONS.labels(decision).inc()
            E2E_LATENCY.observe(latency_ms / 1000.0)
        else:
            DUPLICATES.labels(SERVICE).inc()

        outputs: list[Output] = [(DECISIONS_TOPIC, txn["user_id"], record)]
        if decision != APPROVE:
            outputs.append((ALERTS, txn["user_id"], record))
        return outputs


class FeedbackHandler:
    def __init__(self, db: Database):
        self.db = db

    def handle(self, event: dict) -> list[Output]:
        label = event["label"].upper()
        if label not in VALID_LABELS:
            raise ValueError(f"unknown label {label}")
        self.db.execute(APPLY_LABEL, (label, event["analyst"], event["created_at"], event["txn_id"]))
        FEEDBACK_APPLIED.labels(label).inc()
        return []


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    decisions = StreamProcessor(
        SERVICE, SERVICE, SCORED, DecisionHandler(Database(settings.database_url), settings).handle, settings
    )
    feedback = StreamProcessor(
        FEEDBACK_SERVICE, FEEDBACK_SERVICE, FEEDBACK, FeedbackHandler(Database(settings.database_url)).handle, settings
    )
    run_processors([decisions, feedback], settings.metrics_port)


if __name__ == "__main__":
    main()
