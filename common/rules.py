from collections.abc import Callable
from dataclasses import dataclass

UPI_SINGLE_TXN_LIMIT = 100000.0


@dataclass(frozen=True)
class Rule:
    code: str
    description: str
    score: float
    predicate: Callable[[dict], bool]


RULES = [
    Rule(
        "UPI_LIMIT_EXCEEDED",
        "Amount exceeds the per-transaction UPI limit",
        1.0,
        lambda f: f["amount"] > UPI_SINGLE_TXN_LIMIT,
    ),
    Rule(
        "IMPOSSIBLE_TRAVEL",
        "Location jump faster than a flight, from an unseen device",
        0.9,
        lambda f: f["distance_from_last_km"] > 300 and f["speed_kmh"] > 900 and f["is_new_device"] == 1,
    ),
    Rule(
        "VELOCITY_BURST",
        "Six or more transactions within one minute",
        0.85,
        lambda f: f["txn_count_1m"] >= 6,
    ),
    Rule(
        "MICRO_TXN_PROBING",
        "Burst of tiny payments typical of card testing",
        0.8,
        lambda f: f["txn_count_10m"] >= 8 and f["amount"] < 100,
    ),
    Rule(
        "NEW_DEVICE_HIGH_VALUE_NEW_PAYEE",
        "Large transfer to a new payee from an unseen device",
        0.8,
        lambda f: f["is_new_device"] == 1 and f["is_new_payee"] == 1 and f["amount"] > 25000 and f["user_txn_count"] > 0,
    ),
]

RULES_BY_CODE = {r.code: r for r in RULES}


def evaluate_rules(features: dict) -> tuple[float, list[str]]:
    hits = [r for r in RULES if r.predicate(features)]
    score = max((r.score for r in hits), default=0.0)
    return score, [r.code for r in hits]
