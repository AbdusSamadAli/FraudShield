import logging
import time
from pathlib import Path

from common.config import get_settings
from common.kafka import now_ms
from common.logging_setup import setup_logging
from common.metrics import SCORING_SECONDS
from common.model import MODEL_FILE, FraudModel
from common.processor import Output, StreamProcessor, run_processors
from common.rules import evaluate_rules
from common.topics import ENRICHED, SCORED

log = logging.getLogger("ml-scorer")

SERVICE = "ml-scorer"


class Scorer:
    def __init__(self, model: FraudModel):
        self.model = model

    def handle(self, event: dict) -> list[Output]:
        features = event["features"]
        started = time.perf_counter()
        rule_score, rule_hits = evaluate_rules(features)
        result = self.model.score(features)
        SCORING_SECONDS.observe(time.perf_counter() - started)
        txn = event["transaction"]
        return [
            (
                SCORED,
                txn["user_id"],
                {
                    "transaction": txn,
                    "features": features,
                    "rule_hits": rule_hits,
                    "rule_score": rule_score,
                    "ml_score": result.ml_score,
                    "anomaly_score": result.anomaly_score,
                    "reasons": result.reasons,
                    "model_version": self.model.version,
                    "scored_at": now_ms(),
                },
            )
        ]


def wait_for_model(model_dir: str, timeout: float = 600) -> None:
    deadline = time.time() + timeout
    while not (Path(model_dir) / MODEL_FILE).exists():
        if time.time() > deadline:
            raise TimeoutError(f"no model found in {model_dir}")
        log.info("waiting for model artifacts in %s", model_dir)
        time.sleep(5)


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    wait_for_model(settings.model_dir)
    model = FraudModel.load(settings.model_dir)
    log.info("loaded model %s", model.version)
    processor = StreamProcessor(SERVICE, SERVICE, ENRICHED, Scorer(model).handle, settings)
    run_processors([processor], settings.metrics_port)


if __name__ == "__main__":
    main()
