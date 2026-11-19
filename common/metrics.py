from prometheus_client import Counter, Gauge, Histogram, start_http_server

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

EVENTS_PROCESSED = Counter(
    "fraudshield_events_processed_total", "Events successfully processed", ["service", "topic"]
)
PROCESSING_SECONDS = Histogram(
    "fraudshield_processing_seconds", "Handler processing time", ["service"], buckets=LATENCY_BUCKETS
)
BATCH_SIZE = Histogram(
    "fraudshield_batch_size", "Messages per transactional batch", ["service"], buckets=(1, 5, 10, 25, 50, 100, 200, 500)
)
RETRIES = Counter("fraudshield_retries_total", "Events routed to a retry topic", ["service", "topic"])
DEAD_LETTERS = Counter("fraudshield_dlq_total", "Events routed to a dead letter topic", ["service", "topic"])
TXN_ABORTS = Counter("fraudshield_transaction_aborts_total", "Aborted Kafka transactions", ["service"])
DUPLICATES = Counter("fraudshield_duplicates_total", "Duplicate events detected and skipped", ["service"])

DECISIONS = Counter("fraudshield_decisions_total", "Fraud decisions made", ["decision"])
E2E_LATENCY = Histogram(
    "fraudshield_e2e_latency_seconds", "Event time to decision latency", buckets=LATENCY_BUCKETS
)
SCORING_SECONDS = Histogram("fraudshield_scoring_seconds", "Model inference time", buckets=LATENCY_BUCKETS)

TXN_ACCEPTED = Counter("fraudshield_transactions_accepted_total", "Transactions accepted by the payment API", ["status"])
OUTBOX_PUBLISHED = Counter("fraudshield_outbox_published_total", "Outbox rows published to Kafka")
OUTBOX_PENDING = Gauge("fraudshield_outbox_pending", "Outbox rows waiting to be published")
SIM_SENT = Counter("fraudshield_simulator_sent_total", "Transactions sent by the simulator", ["kind"])
FEEDBACK_APPLIED = Counter("fraudshield_feedback_applied_total", "Analyst labels applied", ["label"])


def serve_metrics(port: int) -> None:
    start_http_server(port)
