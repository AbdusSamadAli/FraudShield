import json
import math
from dataclasses import dataclass, field

HOUR_MS = 3600 * 1000
DAY_MS = 24 * HOUR_MS
MAX_GAP_SECONDS = 30 * 24 * 3600.0
MAX_SPEED_KMH = 5000.0
SAME_METRO_KM = 50.0
MAX_TRACKED_IDS = 50
IST_OFFSET_MS = int(5.5 * HOUR_MS)

HIGH_RISK_CATEGORIES = {"gift_cards", "crypto", "gambling", "electronics"}

FEATURE_NAMES = [
    "amount",
    "log_amount",
    "amount_to_avg_ratio",
    "amount_zscore",
    "txn_count_1m",
    "txn_count_10m",
    "txn_count_1h",
    "amount_sum_10m",
    "amount_sum_1h",
    "seconds_since_last_txn",
    "distance_from_last_km",
    "speed_kmh",
    "is_new_device",
    "is_new_payee",
    "known_device_count",
    "hour_of_day",
    "is_night",
    "account_age_days",
    "is_merchant",
    "is_high_risk_category",
    "user_txn_count",
]


@dataclass
class UserProfile:
    created_at: int
    recent: list[list[float]] = field(default_factory=list)
    devices: list[str] = field(default_factory=list)
    payees: list[str] = field(default_factory=list)
    txn_count: int = 0
    amount_sum: float = 0.0
    amount_sq_sum: float = 0.0
    last_ts: int | None = None
    last_lat: float | None = None
    last_lon: float | None = None

    @classmethod
    def new(cls, txn: dict) -> "UserProfile":
        return cls(created_at=int(txn["account_created_at"]))

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "UserProfile":
        return cls(**json.loads(raw))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def local_hour(ts_ms: int) -> int:
    return int(((ts_ms + IST_OFFSET_MS) // HOUR_MS) % 24)


def _window(recent: list[list[float]], now: int, window_ms: int) -> list[list[float]]:
    return [r for r in recent if now - r[0] <= window_ms]


def compute_features(txn: dict, profile: UserProfile) -> dict[str, float]:
    ts = int(txn["timestamp"])
    amount = float(txn["amount"])

    count = profile.txn_count
    mean = profile.amount_sum / count if count else amount
    variance = profile.amount_sq_sum / count - mean**2 if count else 0.0
    std = math.sqrt(max(variance, 0.0))

    last_1m = _window(profile.recent, ts, 60 * 1000)
    last_10m = _window(profile.recent, ts, 10 * 60 * 1000)
    last_1h = _window(profile.recent, ts, HOUR_MS)

    if profile.last_ts is None:
        gap_seconds = MAX_GAP_SECONDS
        distance = 0.0
    else:
        gap_seconds = min(max((ts - profile.last_ts) / 1000.0, 0.0), MAX_GAP_SECONDS)
        distance = haversine_km(profile.last_lat, profile.last_lon, float(txn["lat"]), float(txn["lon"]))
    if distance > SAME_METRO_KM:
        speed = min(distance / max(gap_seconds / 3600.0, 1 / 60), MAX_SPEED_KMH)
    else:
        speed = 0.0

    hour = local_hour(ts)

    return {
        "amount": amount,
        "log_amount": math.log1p(amount),
        "amount_to_avg_ratio": amount / mean if mean > 0 else 1.0,
        "amount_zscore": (amount - mean) / std if std > 0 else 0.0,
        "txn_count_1m": float(len(last_1m)),
        "txn_count_10m": float(len(last_10m)),
        "txn_count_1h": float(len(last_1h)),
        "amount_sum_10m": float(sum(r[1] for r in last_10m)),
        "amount_sum_1h": float(sum(r[1] for r in last_1h)),
        "seconds_since_last_txn": gap_seconds,
        "distance_from_last_km": distance,
        "speed_kmh": speed,
        "is_new_device": 0.0 if txn["device_id"] in profile.devices else 1.0,
        "is_new_payee": 0.0 if txn["payee_id"] in profile.payees else 1.0,
        "known_device_count": float(len(profile.devices)),
        "hour_of_day": float(hour),
        "is_night": 1.0 if hour < 5 else 0.0,
        "account_age_days": max((ts - profile.created_at) / DAY_MS, 0.0),
        "is_merchant": 1.0 if txn["payee_type"] == "MERCHANT" else 0.0,
        "is_high_risk_category": 1.0 if txn["merchant_category"] in HIGH_RISK_CATEGORIES else 0.0,
        "user_txn_count": float(count),
    }


def _remember(items: list[str], value: str) -> list[str]:
    if value in items:
        items.remove(value)
    items.append(value)
    return items[-MAX_TRACKED_IDS:]


def update_profile(profile: UserProfile, txn: dict) -> UserProfile:
    ts = int(txn["timestamp"])
    amount = float(txn["amount"])
    profile.recent = _window(profile.recent, ts, HOUR_MS) + [[ts, amount]]
    profile.devices = _remember(profile.devices, txn["device_id"])
    profile.payees = _remember(profile.payees, txn["payee_id"])
    profile.txn_count += 1
    profile.amount_sum += amount
    profile.amount_sq_sum += amount * amount
    profile.last_ts = ts
    profile.last_lat = float(txn["lat"])
    profile.last_lon = float(txn["lon"])
    return profile


def feature_vector(features: dict[str, float]) -> list[float]:
    return [float(features[name]) for name in FEATURE_NAMES]
