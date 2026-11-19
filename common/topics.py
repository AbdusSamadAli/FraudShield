RAW = "transactions.raw"
ENRICHED = "transactions.enriched"
SCORED = "transactions.scored"
DECISIONS = "transactions.decisions"
ALERTS = "fraud.alerts"
FEEDBACK = "fraud.feedback"
SIM_CONTROL = "sim.control"

RETRY_SUFFIX = ".retry"
DLQ_SUFFIX = ".dlq"

WEEK_MS = 7 * 24 * 3600 * 1000

PRIMARY_TOPICS = {
    RAW: {"partitions": 6, "config": {"retention.ms": str(WEEK_MS)}},
    ENRICHED: {"partitions": 6, "config": {"retention.ms": str(WEEK_MS)}},
    SCORED: {"partitions": 6, "config": {"retention.ms": str(WEEK_MS)}},
    DECISIONS: {"partitions": 6, "config": {"retention.ms": str(WEEK_MS)}},
    ALERTS: {"partitions": 3, "config": {"retention.ms": str(4 * WEEK_MS)}},
    FEEDBACK: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
    SIM_CONTROL: {"partitions": 1, "config": {"retention.ms": str(3600 * 1000)}},
}

TOPICS_WITH_RETRY = [RAW, ENRICHED, SCORED, FEEDBACK]

TOPIC_SCHEMA = {
    RAW: "transaction",
    ENRICHED: "enriched",
    SCORED: "scored",
    DECISIONS: "decision",
    ALERTS: "decision",
    FEEDBACK: "feedback",
    SIM_CONTROL: "sim_control",
}


def retry_topic(topic: str) -> str:
    return topic + RETRY_SUFFIX


def dlq_topic(topic: str) -> str:
    return topic + DLQ_SUFFIX


def source_topic(topic: str) -> str:
    for suffix in (RETRY_SUFFIX, DLQ_SUFFIX):
        if topic.endswith(suffix):
            return topic[: -len(suffix)]
    return topic


def all_topic_specs() -> dict:
    specs = dict(PRIMARY_TOPICS)
    for topic in TOPICS_WITH_RETRY:
        specs[retry_topic(topic)] = {"partitions": 3, "config": {"retention.ms": str(WEEK_MS)}}
        specs[dlq_topic(topic)] = {"partitions": 1, "config": {"retention.ms": str(4 * WEEK_MS)}}
    return specs
