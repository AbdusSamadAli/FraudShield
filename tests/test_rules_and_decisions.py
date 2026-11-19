import pytest

from common.decision import APPROVE, BLOCK, REVIEW, decide, risk_score
from common.features import FEATURE_NAMES
from common.rules import evaluate_rules


def features(**overrides) -> dict:
    base = {name: 0.0 for name in FEATURE_NAMES}
    base.update({"amount": 500.0, "user_txn_count": 20.0})
    base.update(overrides)
    return base


def test_clean_transaction_hits_no_rules():
    assert evaluate_rules(features()) == (0.0, [])


def test_upi_limit_blocks_outright():
    score, hits = evaluate_rules(features(amount=150000))
    assert score == 1.0
    assert "UPI_LIMIT_EXCEEDED" in hits


def test_impossible_travel_requires_new_device():
    travel = {"distance_from_last_km": 1200.0, "speed_kmh": 4000.0}
    assert "IMPOSSIBLE_TRAVEL" not in evaluate_rules(features(**travel))[1]
    assert "IMPOSSIBLE_TRAVEL" in evaluate_rules(features(**travel, is_new_device=1.0))[1]


def test_micro_probing_and_velocity():
    _, hits = evaluate_rules(features(amount=20, txn_count_10m=9, txn_count_1m=7))
    assert {"MICRO_TXN_PROBING", "VELOCITY_BURST"} <= set(hits)


def test_highest_rule_score_wins():
    score, hits = evaluate_rules(features(amount=150000, txn_count_1m=8))
    assert len(hits) == 2
    assert score == 1.0


@pytest.mark.parametrize(
    "rule,ml,anomaly,expected",
    [(0.0, 0.1, 0.1, APPROVE), (0.0, 0.6, 0.5, REVIEW), (0.0, 0.95, 0.9, BLOCK), (0.9, 0.05, 0.0, BLOCK)],
)
def test_decision_policy(rule, ml, anomaly, expected):
    assert decide(risk_score(rule, ml, anomaly), 0.5, 0.8) == expected


def test_risk_is_bounded():
    assert risk_score(1.0, 1.0, 1.0) == 1.0
    assert risk_score(0.0, 0.0, 0.0) == 0.0
