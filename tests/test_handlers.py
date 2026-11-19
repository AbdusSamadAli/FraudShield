import json
from types import SimpleNamespace

from common.decision import BLOCK
from common.topics import ALERTS, DECISIONS, ENRICHED
from services.decision_service.main import DecisionHandler, FeedbackHandler
from services.feature_engine.main import FeatureEngine


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    def get(self, key):
        return self.store.get(key)

    def pipeline(self, transaction=True):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis):
        self.redis = redis
        self.ops = []

    def set(self, key, value, ex=None):
        self.ops.append((key, value))

    def execute(self):
        for key, value in self.ops:
            self.redis.store[key] = value


class FakeDatabase:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self.updates = []

    def execute(self, sql, params):
        if sql.lstrip().startswith("INSERT"):
            if params["txn_id"] in self.rows:
                return []
            self.rows[params["txn_id"]] = params
            return [(params["txn_id"],)]
        self.updates.append(params)
        return []


def txn(txn_id: str = "t1", ts: int = 1763000000000) -> dict:
    return {
        "txn_id": txn_id,
        "user_id": "U1",
        "amount": 900.0,
        "currency": "INR",
        "payee_id": "P1",
        "payee_type": "P2P",
        "merchant_category": "p2p",
        "channel": "UPI",
        "device_id": "D1",
        "ip_address": "49.1.1.1",
        "lat": 28.6,
        "lon": 77.2,
        "city": "Delhi",
        "account_created_at": ts - 10**10,
        "timestamp": ts,
        "ground_truth": None,
    }


def test_feature_engine_is_idempotent_on_redelivery():
    redis = FakeRedis()
    engine = FeatureEngine(redis)
    first = engine.handle(txn("t1"))
    profile_after_first = redis.store["profile:U1"]
    second = engine.handle(txn("t1"))
    assert first[0][0] == ENRICHED
    assert first[0][2]["features"] == second[0][2]["features"]
    assert redis.store["profile:U1"] == profile_after_first
    assert json.loads(profile_after_first)["txn_count"] == 1


def test_decision_handler_emits_alert_and_dedupes():
    db = FakeDatabase()
    handler = DecisionHandler(db, SimpleNamespace(review_threshold=0.5, block_threshold=0.8))
    event = {
        "transaction": txn("t9"),
        "features": {"amount": 900.0},
        "rule_hits": ["VELOCITY_BURST"],
        "rule_score": 0.85,
        "ml_score": 0.9,
        "anomaly_score": 0.7,
        "reasons": [],
        "model_version": "test",
        "scored_at": 0,
    }
    outputs = handler.handle(event)
    assert [o[0] for o in outputs] == [DECISIONS, ALERTS]
    assert outputs[0][2]["decision"] == BLOCK
    assert db.rows["t9"]["status"] == "OPEN"
    again = handler.handle(event)
    assert len(db.rows) == 1
    assert [o[0] for o in again] == [DECISIONS, ALERTS]


def test_feedback_handler_rejects_unknown_labels():
    handler = FeedbackHandler(FakeDatabase())
    try:
        handler.handle({"txn_id": "t1", "label": "MAYBE", "analyst": "a", "created_at": 0})
    except ValueError:
        return
    raise AssertionError("expected ValueError")
