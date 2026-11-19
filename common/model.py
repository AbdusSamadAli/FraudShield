import json
import math
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import xgboost as xgb

from common.features import FEATURE_NAMES, feature_vector

MODEL_FILE = "xgb_model.json"
IFOREST_FILE = "iforest.joblib"
METADATA_FILE = "metadata.json"

REASON_TEXT = {
    "amount": lambda v: f"High amount of Rs {v:,.0f}",
    "log_amount": lambda v: f"High amount of Rs {math.expm1(v):,.0f}",
    "amount_to_avg_ratio": lambda v: f"Amount is {v:.1f}x the user's average",
    "amount_zscore": lambda v: f"Spend is {v:.1f} std devs from the user's norm",
    "txn_count_1m": lambda v: f"{v:.0f} transactions in the last minute",
    "txn_count_10m": lambda v: f"{v:.0f} transactions in the last 10 minutes",
    "txn_count_1h": lambda v: f"{v:.0f} transactions in the last hour",
    "amount_sum_10m": lambda v: f"Rs {v:,.0f} moved in the last 10 minutes",
    "amount_sum_1h": lambda v: f"Rs {v:,.0f} moved in the last hour",
    "seconds_since_last_txn": lambda v: f"{v:,.0f}s since the previous transaction",
    "distance_from_last_km": lambda v: f"{v:,.0f} km from the previous transaction",
    "speed_kmh": lambda v: f"Implied travel speed of {v:,.0f} km/h",
    "is_new_device": lambda v: "Payment from a never-seen device" if v else "Payment from a known device",
    "is_new_payee": lambda v: "First ever payment to this payee" if v else "Payee seen before",
    "known_device_count": lambda v: f"{v:.0f} devices previously linked to the account",
    "hour_of_day": lambda v: f"Made at {v:02.0f}:00 IST",
    "is_night": lambda v: "Night-time transaction (00:00 to 05:00 IST)" if v else "Daytime transaction",
    "account_age_days": lambda v: f"Account is {v:,.0f} days old",
    "is_merchant": lambda v: "Merchant payment" if v else "Person-to-person transfer",
    "is_high_risk_category": lambda v: "High-risk merchant category" if v else "Low-risk merchant category",
    "user_txn_count": lambda v: f"Only {v:,.0f} prior transactions on the account",
}


@dataclass
class ModelScore:
    ml_score: float
    anomaly_score: float
    reasons: list[dict]


def explain_reason(feature: str, value: float, contribution: float) -> dict:
    return {
        "feature": feature,
        "value": float(value),
        "contribution": round(float(contribution), 4),
        "text": REASON_TEXT[feature](value),
    }


class FraudModel:
    def __init__(self, booster: xgb.Booster, iforest, metadata: dict):
        self.booster = booster
        self.iforest = iforest
        self.metadata = metadata
        self.version = metadata["version"]
        self._anomaly_low = metadata["anomaly_scale"]["low"]
        self._anomaly_high = metadata["anomaly_scale"]["high"]

    @classmethod
    def load(cls, model_dir: str | Path) -> "FraudModel":
        model_dir = Path(model_dir)
        booster = xgb.Booster()
        booster.load_model(str(model_dir / MODEL_FILE))
        booster.set_param({"nthread": 1})
        iforest = joblib.load(model_dir / IFOREST_FILE)
        iforest.n_jobs = 1
        metadata = json.loads((model_dir / METADATA_FILE).read_text())
        return cls(booster, iforest, metadata)

    def normalize_anomaly(self, raw: np.ndarray) -> np.ndarray:
        span = max(self._anomaly_high - self._anomaly_low, 1e-9)
        return np.clip((raw - self._anomaly_low) / span, 0.0, 1.0)

    def score(self, features: dict, top_k: int = 3) -> ModelScore:
        x = np.asarray([feature_vector(features)], dtype=np.float32)
        matrix = xgb.DMatrix(x, feature_names=FEATURE_NAMES)
        probability = float(self.booster.predict(matrix)[0])
        contributions = self.booster.predict(matrix, pred_contribs=True)[0][:-1]
        anomaly = float(self.normalize_anomaly(-self.iforest.score_samples(x))[0])

        ranked = sorted(range(len(FEATURE_NAMES)), key=lambda i: contributions[i], reverse=True)
        reasons = [
            explain_reason(FEATURE_NAMES[i], x[0][i], contributions[i])
            for i in ranked[:top_k]
            if contributions[i] > 0
        ]
        return ModelScore(round(probability, 4), round(anomaly, 4), reasons)
