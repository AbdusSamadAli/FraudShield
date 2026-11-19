import random
import uuid
from dataclasses import dataclass, field
from itertools import accumulate

from common.features import DAY_MS, HOUR_MS, IST_OFFSET_MS, haversine_km


@dataclass(frozen=True)
class City:
    name: str
    lat: float
    lon: float
    weight: float


CITIES = [
    City("Delhi", 28.6139, 77.2090, 0.16),
    City("Mumbai", 19.0760, 72.8777, 0.16),
    City("Bengaluru", 12.9716, 77.5946, 0.14),
    City("Hyderabad", 17.3850, 78.4867, 0.09),
    City("Chennai", 13.0827, 80.2707, 0.08),
    City("Kolkata", 22.5726, 88.3639, 0.08),
    City("Pune", 18.5204, 73.8567, 0.08),
    City("Ahmedabad", 23.0225, 72.5714, 0.06),
    City("Jaipur", 26.9124, 75.7873, 0.05),
    City("Lucknow", 26.8467, 80.9462, 0.05),
    City("Guwahati", 26.1445, 91.7362, 0.03),
    City("Kochi", 9.9312, 76.2673, 0.02),
]

CATEGORY_WEIGHTS = {
    "grocery": 0.22,
    "food": 0.20,
    "fuel": 0.10,
    "utilities": 0.09,
    "shopping": 0.12,
    "pharmacy": 0.06,
    "entertainment": 0.08,
    "travel": 0.05,
    "electronics": 0.05,
    "gift_cards": 0.015,
    "crypto": 0.005,
    "gambling": 0.005,
}

CATEGORY_AMOUNT_MULTIPLIER = {
    "grocery": 0.8,
    "food": 0.5,
    "fuel": 0.7,
    "utilities": 1.3,
    "shopping": 1.4,
    "pharmacy": 0.6,
    "entertainment": 0.7,
    "travel": 3.0,
    "electronics": 3.5,
    "gift_cards": 1.0,
    "crypto": 2.0,
    "gambling": 1.0,
}

HOUR_WEIGHTS = [1, 0.6, 0.4, 0.3, 0.3, 0.6, 1.5, 3, 4.5, 5.5, 6, 6.5, 7, 6.5, 6, 6, 6.5, 7, 7.5, 8, 7.5, 6, 4, 2]

FRAUD_PATTERNS = [
    "card_testing",
    "impossible_travel",
    "account_takeover",
    "velocity_burst",
    "high_value_new_payee",
]

MAX_NORMAL_AMOUNT = 95000.0


@dataclass
class SimUser:
    user_id: str
    home: int
    devices: list[str]
    avg_amount: float
    payees: list[str]
    created_at: int
    ip_prefix: str
    activity: float = 1.0
    merchants: list[str] = field(default_factory=list)


class TransactionFactory:
    def __init__(self, n_users: int, seed: int, reference_ms: int):
        self.rng = random.Random(seed)
        self.reference_ms = reference_ms
        self.categories = list(CATEGORY_WEIGHTS)
        self.category_cum = list(accumulate(CATEGORY_WEIGHTS.values()))
        self.city_cum = list(accumulate(c.weight for c in CITIES))
        self.merchants = {cat: [f"M-{cat}-{i:03d}" for i in range(40)] for cat in self.categories}
        self.merchant_category = {m: cat for cat, ms in self.merchants.items() for m in ms}
        self.users = [self._make_user(i, n_users) for i in range(n_users)]
        self.user_cum = list(accumulate(u.activity for u in self.users))

    def _make_user(self, index: int, n_users: int) -> SimUser:
        rng = self.rng
        user_id = f"U{index:06d}"
        home = rng.choices(range(len(CITIES)), cum_weights=self.city_cum)[0]
        device_count = rng.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        favourite_cats = rng.choices(self.categories, cum_weights=self.category_cum, k=6)
        return SimUser(
            user_id=user_id,
            home=home,
            devices=[f"D-{user_id}-{k}" for k in range(device_count)],
            avg_amount=min(max(rng.lognormvariate(6.6, 0.75), 80.0), 15000.0),
            payees=[f"U{rng.randrange(n_users):06d}" for _ in range(rng.randint(4, 20))],
            created_at=self.reference_ms - int(rng.uniform(20, 2500) * DAY_MS),
            ip_prefix=f"49.{rng.randrange(32, 220)}.{rng.randrange(0, 255)}",
            activity=rng.lognormvariate(0, 0.6),
            merchants=[rng.choice(self.merchants[c]) for c in favourite_cats],
        )

    def random_user(self) -> SimUser:
        return self.rng.choices(self.users, cum_weights=self.user_cum)[0]

    def _txn(
        self,
        user: SimUser,
        ts: int,
        payee_id: str,
        category: str,
        amount: float,
        device: str,
        city: int,
        ip: str,
        label: str | None,
    ) -> dict:
        place = CITIES[city]
        return {
            "txn_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "user_id": user.user_id,
            "amount": round(max(amount, 1.0), 2),
            "currency": "INR",
            "payee_id": payee_id,
            "payee_type": "P2P" if category == "p2p" else "MERCHANT",
            "merchant_category": category,
            "channel": "UPI",
            "device_id": device,
            "ip_address": ip,
            "lat": round(place.lat + self.rng.uniform(-0.08, 0.08), 5),
            "lon": round(place.lon + self.rng.uniform(-0.08, 0.08), 5),
            "city": place.name,
            "account_created_at": user.created_at,
            "timestamp": int(ts),
            "ground_truth": label,
        }

    def _user_ip(self, user: SimUser) -> str:
        return f"{user.ip_prefix}.{self.rng.randrange(1, 255)}"

    def _attacker_ip(self) -> str:
        return f"185.{self.rng.randrange(0, 255)}.{self.rng.randrange(0, 255)}.{self.rng.randrange(1, 255)}"

    def _far_city(self, home: int, min_km: float = 800.0) -> int:
        h = CITIES[home]
        options = [i for i, c in enumerate(CITIES) if haversine_km(h.lat, h.lon, c.lat, c.lon) >= min_km]
        return self.rng.choice(options or [i for i in range(len(CITIES)) if i != home])

    def normal(self, user: SimUser, ts: int) -> dict:
        rng = self.rng
        if rng.random() < 0.62:
            if rng.random() < 0.7:
                payee = rng.choice(user.merchants)
            else:
                category = rng.choices(self.categories, cum_weights=self.category_cum)[0]
                payee = rng.choice(self.merchants[category])
            category = self.merchant_category[payee]
        else:
            category = "p2p"
            payee = rng.choice(user.payees) if rng.random() < 0.85 else f"U{rng.randrange(len(self.users)):06d}"

        amount = user.avg_amount * rng.lognormvariate(0, 0.7) * CATEGORY_AMOUNT_MULTIPLIER.get(category, 1.0)
        if rng.random() < 0.015:
            amount *= rng.uniform(4, 10)
        amount = min(amount, MAX_NORMAL_AMOUNT)

        if rng.random() < 0.03:
            device = f"D-{user.user_id}-{len(user.devices)}"
            user.devices.append(device)
        else:
            device = rng.choice(user.devices)

        city = user.home if rng.random() > 0.005 else rng.randrange(len(CITIES))
        return self._txn(user, ts, payee, category, amount, device, city, self._user_ip(user), None)

    def card_testing(self, user: SimUser, ts: int) -> list[dict]:
        rng = self.rng
        device = f"D-atk-{rng.getrandbits(32):08x}"
        city = rng.randrange(len(CITIES))
        ip = self._attacker_ip()
        txns = []
        for _ in range(rng.randint(8, 14)):
            ts += int(rng.uniform(10, 60) * 1000)
            category = rng.choice(["gift_cards", "shopping", "entertainment", "electronics"])
            payee = rng.choice(self.merchants[category])
            txns.append(self._txn(user, ts, payee, category, rng.uniform(1, 60), device, city, ip, "card_testing"))
        for _ in range(rng.randint(1, 2)):
            ts += int(rng.uniform(20, 90) * 1000)
            category = rng.choice(["electronics", "gift_cards"])
            amount = rng.uniform(4000, 40000)
            txns.append(self._txn(user, ts, rng.choice(self.merchants[category]), category, amount, device, city, ip, "card_testing"))
        return txns

    def impossible_travel(self, user: SimUser, ts: int, gap_minutes: tuple[float, float] = (5, 45)) -> list[dict]:
        rng = self.rng
        anchor = self.normal(user, ts)
        anchor["ground_truth"] = None
        far = self._far_city(user.home)
        fraud_ts = ts + int(rng.uniform(*gap_minutes) * 60 * 1000)
        category = rng.choice(["electronics", "shopping", "travel", "gift_cards"])
        amount = min(user.avg_amount * rng.uniform(2, 9), MAX_NORMAL_AMOUNT)
        device = f"D-atk-{rng.getrandbits(32):08x}" if rng.random() < 0.8 else rng.choice(user.devices)
        fraud = self._txn(
            user, fraud_ts, rng.choice(self.merchants[category]), category, amount, device, far,
            self._attacker_ip(), "impossible_travel",
        )
        return [anchor, fraud]

    def account_takeover(self, user: SimUser, ts: int) -> list[dict]:
        rng = self.rng
        device = f"D-atk-{rng.getrandbits(32):08x}"
        mule = f"U-mule-{rng.getrandbits(24):06x}"
        city = user.home if rng.random() < 0.5 else rng.randrange(len(CITIES))
        ip = self._attacker_ip()
        txns = []
        for _ in range(rng.randint(1, 4)):
            ts += int(rng.uniform(30, 240) * 1000)
            amount = min(user.avg_amount * rng.uniform(4, 25), 99000.0)
            txns.append(self._txn(user, ts, mule, "p2p", amount, device, city, ip, "account_takeover"))
        return txns

    def velocity_burst(self, user: SimUser, ts: int) -> list[dict]:
        rng = self.rng
        device = rng.choice(user.devices) if rng.random() < 0.5 else f"D-atk-{rng.getrandbits(32):08x}"
        ip = self._user_ip(user) if device in user.devices else self._attacker_ip()
        txns = []
        for _ in range(rng.randint(6, 12)):
            ts += int(rng.uniform(4, 15) * 1000)
            payee = f"U-mule-{rng.getrandbits(24):06x}"
            txns.append(self._txn(user, ts, payee, "p2p", rng.uniform(500, 6000), device, user.home, ip, "velocity_burst"))
        return txns

    def high_value_new_payee(self, user: SimUser, ts: int) -> list[dict]:
        rng = self.rng
        known_device = rng.random() < 0.6
        device = rng.choice(user.devices) if known_device else f"D-atk-{rng.getrandbits(32):08x}"
        ip = self._user_ip(user) if known_device else self._attacker_ip()
        if rng.random() < 0.5:
            category = rng.choice(["crypto", "gift_cards", "gambling"])
            payee = rng.choice(self.merchants[category])
        else:
            category = "p2p"
            payee = f"U-mule-{rng.getrandbits(24):06x}"
        amount = min(max(user.avg_amount * rng.uniform(8, 40), 15000.0), 99000.0)
        return [self._txn(user, ts, payee, category, amount, device, user.home, ip, "high_value_new_payee")]

    def attack(self, pattern: str, user: SimUser, ts: int, fast: bool = False) -> list[dict]:
        if pattern == "impossible_travel":
            return self.impossible_travel(user, ts, (0.3, 1.0) if fast else (5, 45))
        return getattr(self, pattern)(user, ts)

    def _sample_ts(self, start_ms: int, days: int, night_bias: bool = False) -> int:
        rng = self.rng
        day = rng.randrange(days)
        if night_bias and rng.random() < 0.6:
            hour = rng.randrange(0, 5)
        else:
            hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        return start_ms + day * DAY_MS + hour * HOUR_MS + int(rng.random() * HOUR_MS) - IST_OFFSET_MS

    def generate(self, n_events: int, days: int, fraud_rate: float, start_ms: int) -> list[dict]:
        rng = self.rng
        target_fraud = int(n_events * fraud_rate)
        events: list[dict] = []
        fraud_count = 0
        while fraud_count < target_fraud:
            pattern = rng.choice(FRAUD_PATTERNS)
            ts = self._sample_ts(start_ms, days, night_bias=pattern == "account_takeover")
            seq = self.attack(pattern, self.random_user(), ts)
            fraud_count += sum(1 for t in seq if t["ground_truth"])
            events.extend(seq)
        while len(events) < n_events:
            events.append(self.normal(self.random_user(), self._sample_ts(start_ms, days)))
        events.sort(key=lambda t: t["timestamp"])
        return events
