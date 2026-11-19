import json

import pytest

from common.features import (
    FEATURE_NAMES,
    UserProfile,
    compute_features,
    feature_vector,
    haversine_km,
    local_hour,
    update_profile,
)

BASE_TS = 1763000000000


def make_txn(ts: int, amount: float = 500.0, device: str = "D1", payee: str = "P1", lat=28.61, lon=77.21) -> dict:
    return {
        "txn_id": f"t-{ts}",
        "user_id": "U1",
        "amount": amount,
        "payee_id": payee,
        "payee_type": "P2P",
        "merchant_category": "p2p",
        "device_id": device,
        "lat": lat,
        "lon": lon,
        "account_created_at": BASE_TS - 100 * 24 * 3600 * 1000,
        "timestamp": ts,
    }


def build_profile(txns: list[dict]) -> UserProfile:
    profile = UserProfile.new(txns[0])
    for txn in txns:
        update_profile(profile, txn)
    return profile


def test_feature_vector_has_every_feature_in_order():
    features = compute_features(make_txn(BASE_TS), UserProfile.new(make_txn(BASE_TS)))
    assert list(features) == FEATURE_NAMES
    assert len(feature_vector(features)) == len(FEATURE_NAMES)


def test_first_transaction_has_no_history():
    features = compute_features(make_txn(BASE_TS), UserProfile.new(make_txn(BASE_TS)))
    assert features["user_txn_count"] == 0
    assert features["is_new_device"] == 1
    assert features["distance_from_last_km"] == 0
    assert features["speed_kmh"] == 0
    assert features["account_age_days"] == pytest.approx(100)


def test_velocity_windows_count_recent_transactions_only():
    history = [make_txn(BASE_TS + i * 10_000) for i in range(5)]
    history.insert(0, make_txn(BASE_TS - 2 * 3600 * 1000))
    profile = build_profile(history)
    features = compute_features(make_txn(BASE_TS + 55_000), profile)
    assert features["txn_count_1m"] == 5
    assert features["txn_count_1h"] == 5


def test_amount_ratio_against_user_average():
    profile = build_profile([make_txn(BASE_TS + i, amount=1000) for i in range(10)])
    features = compute_features(make_txn(BASE_TS + 60_000, amount=8000), profile)
    assert features["amount_to_avg_ratio"] == pytest.approx(8.0)


def test_impossible_travel_speed():
    profile = build_profile([make_txn(BASE_TS)])
    mumbai = make_txn(BASE_TS + 10 * 60 * 1000, lat=19.07, lon=72.88)
    features = compute_features(mumbai, profile)
    assert features["distance_from_last_km"] == pytest.approx(1150, rel=0.02)
    assert features["speed_kmh"] > 5000 - 1


def test_same_city_jitter_does_not_create_speed():
    profile = build_profile([make_txn(BASE_TS)])
    nearby = make_txn(BASE_TS + 30_000, lat=28.68, lon=77.15)
    assert compute_features(nearby, profile)["speed_kmh"] == 0


def test_known_device_and_payee_are_remembered():
    profile = build_profile([make_txn(BASE_TS, device="D9", payee="P9")])
    features = compute_features(make_txn(BASE_TS + 1000, device="D9", payee="P9"), profile)
    assert features["is_new_device"] == 0
    assert features["is_new_payee"] == 0


def test_profile_round_trips_through_json():
    profile = build_profile([make_txn(BASE_TS + i * 1000) for i in range(3)])
    restored = UserProfile.from_json(profile.to_json())
    assert restored == profile
    assert json.loads(profile.to_json())["txn_count"] == 3


def test_local_hour_is_ist():
    midnight_utc = 1763078400000
    assert local_hour(midnight_utc) == 5


def test_haversine_delhi_mumbai():
    assert haversine_km(28.6139, 77.2090, 19.0760, 72.8777) == pytest.approx(1150, rel=0.02)
