import logging
import socket
import time

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from common.config import Settings
from common.schemas import schema_str
from common.topics import TOPIC_SCHEMA, source_topic

log = logging.getLogger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


def instance_id() -> str:
    return socket.gethostname()


def producer_config(settings: Settings, client_id: str, transactional_id: str | None = None) -> dict:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "compression.type": "lz4",
        "delivery.timeout.ms": 30000,
    }
    if transactional_id:
        conf["transactional.id"] = transactional_id
        conf["transaction.timeout.ms"] = 60000
    return conf


def consumer_config(settings: Settings, group_id: str, **overrides) -> dict:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "client.id": f"{group_id}-{instance_id()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 15000,
        "max.poll.interval.ms": 300000,
    }
    conf.update(overrides)
    return conf


class Serde:
    def __init__(self, settings: Settings):
        self.registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._serializers: dict[str, AvroSerializer] = {}
        self._deserializer = AvroDeserializer(self.registry)

    def _serializer(self, schema_name: str) -> AvroSerializer:
        if schema_name not in self._serializers:
            self._serializers[schema_name] = AvroSerializer(
                self.registry, schema_str(schema_name), conf={"auto.register.schemas": True}
            )
        return self._serializers[schema_name]

    def serialize(self, topic: str, value: dict) -> bytes:
        schema_name = TOPIC_SCHEMA[source_topic(topic)]
        return self._serializer(schema_name)(value, SerializationContext(topic, MessageField.VALUE))

    def deserialize(self, topic: str, data: bytes) -> dict:
        return self._deserializer(data, SerializationContext(topic, MessageField.VALUE))


class EventPublisher:
    def __init__(self, settings: Settings, client_id: str, serde: Serde | None = None):
        self.serde = serde or Serde(settings)
        self.producer = Producer(producer_config(settings, client_id))
        self.failures: list[str] = []

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failures.append(str(err))
            log.error("delivery failed topic=%s error=%s", msg.topic(), err)

    def publish(self, topic: str, key: str, value: dict, headers: list | None = None) -> None:
        payload = self.serde.serialize(topic, value)
        while True:
            try:
                self.producer.produce(
                    topic, key=key.encode(), value=payload, headers=headers, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self.producer.poll(0.1)
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> list[str]:
        remaining = self.producer.flush(timeout)
        failures, self.failures = self.failures, []
        if remaining:
            failures.append(f"{remaining} messages not delivered before timeout")
        return failures


def make_consumer(settings: Settings, group_id: str, **overrides) -> Consumer:
    return Consumer(consumer_config(settings, group_id, **overrides))


def is_fatal(err: KafkaError) -> bool:
    return err.fatal() or err.code() in (KafkaError._FENCED, KafkaError.TOPIC_AUTHORIZATION_FAILED)
