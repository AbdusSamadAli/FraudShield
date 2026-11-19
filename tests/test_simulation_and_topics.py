import io

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from common.schemas import SCHEMAS
from common.simulation import FRAUD_PATTERNS, TransactionFactory
from common.topics import (
    TOPIC_SCHEMA,
    TOPICS_WITH_RETRY,
    all_topic_specs,
    dlq_topic,
    retry_topic,
    source_topic,
)

REFERENCE = 1763000000000


def round_trip(schema_name: str, record: dict) -> dict:
    schema = parse_schema(SCHEMAS[schema_name])
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record)
    buffer.seek(0)
    return schemaless_reader(buffer, schema)


def test_factory_is_deterministic():
    a = TransactionFactory(50, seed=1, reference_ms=REFERENCE)
    b = TransactionFactory(50, seed=1, reference_ms=REFERENCE)
    assert a.normal(a.users[0], REFERENCE) == b.normal(b.users[0], REFERENCE)


def test_every_pattern_produces_labelled_fraud():
    factory = TransactionFactory(50, seed=2, reference_ms=REFERENCE)
    for pattern in FRAUD_PATTERNS:
        sequence = factory.attack(pattern, factory.users[1], REFERENCE)
        labels = {t["ground_truth"] for t in sequence}
        assert pattern in labels
        assert [t["timestamp"] for t in sequence] == sorted(t["timestamp"] for t in sequence)


def test_generated_dataset_has_target_fraud_rate():
    factory = TransactionFactory(200, seed=3, reference_ms=REFERENCE)
    events = factory.generate(5000, days=5, fraud_rate=0.02, start_ms=REFERENCE)
    fraud = sum(1 for e in events if e["ground_truth"])
    assert len(events) >= 5000
    assert 0.015 <= fraud / len(events) <= 0.03


def test_transaction_matches_avro_schema():
    factory = TransactionFactory(10, seed=4, reference_ms=REFERENCE)
    txn = factory.normal(factory.users[0], REFERENCE)
    assert round_trip("transaction", txn) == txn


def test_all_schemas_parse():
    for schema in SCHEMAS.values():
        parse_schema(schema)


def test_retry_and_dlq_topics_exist_for_processed_streams():
    specs = all_topic_specs()
    for topic in TOPICS_WITH_RETRY:
        assert retry_topic(topic) in specs
        assert dlq_topic(topic) in specs
        assert source_topic(retry_topic(topic)) == topic
        assert source_topic(dlq_topic(topic)) == topic


def test_every_primary_topic_has_a_schema():
    for topic in TOPIC_SCHEMA:
        assert TOPIC_SCHEMA[topic] in SCHEMAS
