import json
import logging

import redis

from common.config import get_settings
from common.features import UserProfile, compute_features, update_profile
from common.kafka import now_ms
from common.logging_setup import setup_logging
from common.metrics import DUPLICATES
from common.processor import Output, StreamProcessor, run_processors
from common.topics import ENRICHED, RAW

log = logging.getLogger("feature-engine")

SERVICE = "feature-engine"
PROFILE_TTL_SECONDS = 90 * 24 * 3600
FEATURE_CACHE_TTL_SECONDS = 24 * 3600


class FeatureEngine:
    def __init__(self, client: redis.Redis):
        self.redis = client

    def features_for(self, txn: dict) -> dict:
        cache_key = f"features:{txn['txn_id']}"
        cached = self.redis.get(cache_key)
        if cached is not None:
            DUPLICATES.labels(SERVICE).inc()
            return json.loads(cached)

        profile_key = f"profile:{txn['user_id']}"
        raw = self.redis.get(profile_key)
        profile = UserProfile.from_json(raw) if raw else UserProfile.new(txn)
        features = compute_features(txn, profile)
        update_profile(profile, txn)

        pipe = self.redis.pipeline(transaction=True)
        pipe.set(profile_key, profile.to_json(), ex=PROFILE_TTL_SECONDS)
        pipe.set(cache_key, json.dumps(features), ex=FEATURE_CACHE_TTL_SECONDS)
        pipe.execute()
        return features

    def handle(self, txn: dict) -> list[Output]:
        features = self.features_for(txn)
        return [(ENRICHED, txn["user_id"], {"transaction": txn, "features": features, "enriched_at": now_ms()})]


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    engine = FeatureEngine(redis.Redis.from_url(settings.redis_url))
    processor = StreamProcessor(SERVICE, SERVICE, RAW, engine.handle, settings)
    run_processors([processor], settings.metrics_port)


if __name__ == "__main__":
    main()
