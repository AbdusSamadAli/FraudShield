import argparse
import json
import logging
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from common.config import get_settings
from common.decision import ANOMALY_WEIGHT, ML_WEIGHT
from common.features import FEATURE_NAMES, UserProfile, compute_features, update_profile
from common.logging_setup import setup_logging
from common.model import IFOREST_FILE, METADATA_FILE, MODEL_FILE
from common.rules import evaluate_rules
from common.simulation import TransactionFactory

log = logging.getLogger("trainer")

START_MS = 1759257000000


def build_dataset(n_events: int, n_users: int, fraud_rate: float, days: int, seed: int) -> pd.DataFrame:
    factory = TransactionFactory(n_users, seed, reference_ms=START_MS)
    events = factory.generate(n_events, days, fraud_rate, START_MS)
    profiles: dict[str, UserProfile] = {}
    rows = []
    for txn in events:
        profile = profiles.get(txn["user_id"]) or UserProfile.new(txn)
        features = compute_features(txn, profile)
        profiles[txn["user_id"]] = update_profile(profile, txn)
        features["label"] = 1 if txn["ground_truth"] else 0
        features["pattern"] = txn["ground_truth"] or "legit"
        features["timestamp"] = txn["timestamp"]
        rows.append(features)
    return pd.DataFrame(rows)


def load_feedback(database_url: str) -> pd.DataFrame:
    import psycopg

    with psycopg.connect(database_url) as conn:
        rows = conn.execute(
            "SELECT features, label, event_ts FROM decisions WHERE label IS NOT NULL"
        ).fetchall()
    records = []
    for features, label, event_ts in rows:
        record = {name: float(features.get(name, 0.0)) for name in FEATURE_NAMES}
        record["label"] = 1 if label == "FRAUD" else 0
        record["pattern"] = "analyst_feedback"
        record["timestamp"] = event_ts
        records.append(record)
    return pd.DataFrame(records)


def classification_metrics(y_true: np.ndarray, scores: np.ndarray, threshold: float) -> dict:
    flagged = (scores >= threshold).astype(int)
    return {
        "pr_auc": round(float(average_precision_score(y_true, scores)), 4),
        "roc_auc": round(float(roc_auc_score(y_true, scores)), 4),
        "precision": round(float(precision_score(y_true, flagged, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, flagged, zero_division=0)), 4),
        "f1": round(float(f1_score(y_true, flagged, zero_division=0)), 4),
        "flag_rate": round(float(flagged.mean()), 4),
    }


def train(args: argparse.Namespace) -> dict:
    settings = get_settings()
    started = time.time()
    log.info("generating %s synthetic transactions", args.events)
    data = build_dataset(args.events, args.users, args.fraud_rate, args.days, args.seed)

    if args.with_feedback:
        feedback = load_feedback(settings.database_url)
        log.info("adding %s analyst-labelled transactions", len(feedback))
        data = pd.concat([data, feedback], ignore_index=True).sort_values("timestamp", kind="stable")

    split = int(len(data) * 0.8)
    train_df, test_df = data.iloc[:split], data.iloc[split:]
    valid_cut = int(len(train_df) * 0.9)
    fit_df, valid_df = train_df.iloc[:valid_cut], train_df.iloc[valid_cut:]

    x_fit = fit_df[FEATURE_NAMES].astype(np.float32)
    x_valid = valid_df[FEATURE_NAMES].astype(np.float32)
    x_test = test_df[FEATURE_NAMES].astype(np.float32)
    y_fit, y_valid, y_test = fit_df["label"].values, valid_df["label"].values, test_df["label"].values

    pos_weight = float((y_fit == 0).sum() / max((y_fit == 1).sum(), 1))
    params = {
        "n_estimators": 500,
        "max_depth": 6,
        "learning_rate": 0.06,
        "subsample": 0.9,
        "colsample_bytree": 0.9,
        "min_child_weight": 2,
        "scale_pos_weight": round(pos_weight, 2),
        "eval_metric": "aucpr",
        "early_stopping_rounds": 40,
        "tree_method": "hist",
        "random_state": args.seed,
    }
    log.info("training XGBoost with scale_pos_weight=%.1f", pos_weight)
    classifier = xgb.XGBClassifier(**params, n_jobs=-1)
    classifier.fit(x_fit, y_fit, eval_set=[(x_valid, y_valid)], verbose=False)

    log.info("training Isolation Forest on legitimate traffic")
    legit = train_df[train_df["label"] == 0][FEATURE_NAMES].astype(np.float32)
    iforest = IsolationForest(n_estimators=200, max_samples=4096, random_state=args.seed, n_jobs=-1)
    iforest.fit(legit.values)
    raw_legit = -iforest.score_samples(legit.values)
    anomaly_low, anomaly_high = float(np.quantile(raw_legit, 0.5)), float(np.quantile(raw_legit, 0.999))

    ml_scores = classifier.predict_proba(x_test)[:, 1]
    anomaly = np.clip((-iforest.score_samples(x_test.values) - anomaly_low) / (anomaly_high - anomaly_low), 0, 1)
    rule_scores = np.array([evaluate_rules(row)[0] for row in test_df[FEATURE_NAMES].to_dict("records")])
    combined = np.minimum(np.maximum(rule_scores, ML_WEIGHT * ml_scores + ANOMALY_WEIGHT * anomaly), 1.0)

    review, block = settings.review_threshold, settings.block_threshold
    flagged = combined >= review
    tn, fp, fn, tp = confusion_matrix(y_test, flagged.astype(int), labels=[0, 1]).ravel()

    precision_curve, recall_curve, _ = precision_recall_curve(y_test, combined)
    step = max(len(precision_curve) // 60, 1)
    pr_points = [
        {"precision": round(float(p), 4), "recall": round(float(r), 4)}
        for p, r in zip(precision_curve[::step], recall_curve[::step])
    ]

    pattern_recall = {
        pattern: round(float(flagged[(test_df["pattern"] == pattern).values].mean()), 4)
        for pattern in sorted(test_df["pattern"].unique())
        if pattern != "legit"
    }

    gain = classifier.get_booster().get_score(importance_type="gain")
    total_gain = sum(gain.values()) or 1.0
    importance = dict(
        sorted(((k, round(v / total_gain, 4)) for k, v in gain.items()), key=lambda kv: kv[1], reverse=True)
    )

    decisions = np.where(combined >= block, "BLOCK", np.where(combined >= review, "REVIEW", "APPROVE"))
    metadata = {
        "version": time.strftime("v%Y%m%d-%H%M%S"),
        "trained_at": int(time.time() * 1000),
        "training_seconds": round(time.time() - started, 1),
        "features": FEATURE_NAMES,
        "anomaly_scale": {"low": anomaly_low, "high": anomaly_high},
        "thresholds": {"review": review, "block": block},
        "blend": {"ml_weight": ML_WEIGHT, "anomaly_weight": ANOMALY_WEIGHT},
        "dataset": {
            "events": int(len(data)),
            "users": args.users,
            "days": args.days,
            "train_rows": int(len(train_df)),
            "test_rows": int(len(test_df)),
            "fraud_rate": round(float(data["label"].mean()), 4),
            "split": "temporal 80/20",
        },
        "params": {**params, "best_iteration": int(classifier.best_iteration)},
        "metrics": {
            "combined": classification_metrics(y_test, combined, review),
            "xgboost_only": classification_metrics(y_test, ml_scores, review),
            "rules_only": classification_metrics(y_test, rule_scores, review),
            "isolation_forest_only": classification_metrics(y_test, anomaly, review),
        },
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        "false_positive_rate": round(float(fp / max(fp + tn, 1)), 5),
        "decision_mix": {d: int((decisions == d).sum()) for d in ("APPROVE", "REVIEW", "BLOCK")},
        "pattern_recall": pattern_recall,
        "feature_importance": importance,
        "pr_curve": pr_points,
    }

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    classifier.save_model(out / MODEL_FILE)
    joblib.dump(iforest, out / IFOREST_FILE)
    (out / METADATA_FILE).write_text(json.dumps(metadata, indent=2))
    log.info("saved model %s to %s", metadata["version"], out)
    log.info("combined metrics %s", metadata["metrics"]["combined"])
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the FraudShield models")
    parser.add_argument("--out", default=get_settings().model_dir)
    parser.add_argument("--events", type=int, default=200000)
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--with-feedback", action="store_true")
    parser.add_argument("--if-missing", action="store_true")
    args = parser.parse_args()

    setup_logging(get_settings().log_level)
    if args.if_missing and (Path(args.out) / MODEL_FILE).exists():
        log.info("model already present in %s, skipping training", args.out)
        return
    train(args)


if __name__ == "__main__":
    main()
