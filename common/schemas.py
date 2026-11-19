import json

NAMESPACE = "com.fraudshield.events"

TRANSACTION = {
    "type": "record",
    "name": "Transaction",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "txn_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string", "default": "INR"},
        {"name": "payee_id", "type": "string"},
        {"name": "payee_type", "type": "string"},
        {"name": "merchant_category", "type": "string"},
        {"name": "channel", "type": "string", "default": "UPI"},
        {"name": "device_id", "type": "string"},
        {"name": "ip_address", "type": "string"},
        {"name": "lat", "type": "double"},
        {"name": "lon", "type": "double"},
        {"name": "city", "type": "string"},
        {"name": "account_created_at", "type": "long"},
        {"name": "timestamp", "type": "long"},
        {"name": "ground_truth", "type": ["null", "string"], "default": None},
    ],
}

REASON = {
    "type": "record",
    "name": "Reason",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "feature", "type": "string"},
        {"name": "value", "type": "double"},
        {"name": "contribution", "type": "double"},
        {"name": "text", "type": "string"},
    ],
}

ENRICHED = {
    "type": "record",
    "name": "EnrichedTransaction",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "transaction", "type": TRANSACTION},
        {"name": "features", "type": {"type": "map", "values": "double"}},
        {"name": "enriched_at", "type": "long"},
    ],
}

SCORED = {
    "type": "record",
    "name": "ScoredTransaction",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "transaction", "type": TRANSACTION},
        {"name": "features", "type": {"type": "map", "values": "double"}},
        {"name": "rule_hits", "type": {"type": "array", "items": "string"}},
        {"name": "rule_score", "type": "double"},
        {"name": "ml_score", "type": "double"},
        {"name": "anomaly_score", "type": "double"},
        {"name": "reasons", "type": {"type": "array", "items": REASON}},
        {"name": "model_version", "type": "string"},
        {"name": "scored_at", "type": "long"},
    ],
}

DECISION = {
    "type": "record",
    "name": "Decision",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "transaction", "type": TRANSACTION},
        {"name": "decision", "type": "string"},
        {"name": "risk_score", "type": "double"},
        {"name": "rule_score", "type": "double"},
        {"name": "ml_score", "type": "double"},
        {"name": "anomaly_score", "type": "double"},
        {"name": "rule_hits", "type": {"type": "array", "items": "string"}},
        {"name": "reasons", "type": {"type": "array", "items": REASON}},
        {"name": "features", "type": {"type": "map", "values": "double"}},
        {"name": "model_version", "type": "string"},
        {"name": "latency_ms", "type": "double"},
        {"name": "decided_at", "type": "long"},
    ],
}

FEEDBACK = {
    "type": "record",
    "name": "AnalystFeedback",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "txn_id", "type": "string"},
        {"name": "label", "type": "string"},
        {"name": "analyst", "type": "string"},
        {"name": "notes", "type": ["null", "string"], "default": None},
        {"name": "created_at", "type": "long"},
    ],
}

SIM_CONTROL = {
    "type": "record",
    "name": "SimulatorCommand",
    "namespace": NAMESPACE,
    "fields": [
        {"name": "command", "type": "string"},
        {"name": "pattern", "type": ["null", "string"], "default": None},
        {"name": "count", "type": "int", "default": 1},
        {"name": "issued_at", "type": "long"},
    ],
}

SCHEMAS = {
    "transaction": TRANSACTION,
    "enriched": ENRICHED,
    "scored": SCORED,
    "decision": DECISION,
    "feedback": FEEDBACK,
    "sim_control": SIM_CONTROL,
}


def schema_str(name: str) -> str:
    return json.dumps(SCHEMAS[name])
