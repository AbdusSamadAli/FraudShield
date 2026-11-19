import threading
import time

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from common.config import Settings
from common.kafka import make_consumer
from common.topics import ENRICHED, FEEDBACK, RAW, SCORED, TOPICS_WITH_RETRY, dlq_topic, retry_topic

CONSUMER_GROUPS = {
    "feature-engine": RAW,
    "ml-scorer": ENRICHED,
    "decision-service": SCORED,
    "decision-feedback": FEEDBACK,
}

CACHE_SECONDS = 3.0


class KafkaMonitor:
    def __init__(self, settings: Settings):
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self.watermarks = make_consumer(settings, "dashboard-watermarks")
        self._cache: tuple[float, dict] | None = None
        self._lock = threading.Lock()

    def _high_low(self, topic: str, partition: int) -> tuple[int, int]:
        return self.watermarks.get_watermark_offsets(TopicPartition(topic, partition), timeout=5, cached=False)

    def _partitions(self, metadata, topic: str) -> list[int]:
        return list(metadata.topics[topic].partitions) if topic in metadata.topics else []

    def _group_report(self, metadata, group: str, topic: str) -> dict:
        topics = [topic, retry_topic(topic)]
        requested = [TopicPartition(t, p) for t in topics for p in self._partitions(metadata, t)]
        offsets = self.admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, requested)])
        committed = offsets[group].result(timeout=5).topic_partitions

        partitions = []
        total_lag = 0
        for tp in committed:
            low, high = self._high_low(tp.topic, tp.partition)
            position = tp.offset if tp.offset >= 0 else low
            lag = max(high - position, 0)
            total_lag += lag
            if tp.topic == topic:
                partitions.append({"partition": tp.partition, "committed": position, "end": high, "lag": lag})

        description = self.admin.describe_consumer_groups([group])[group].result(timeout=5)
        return {
            "group": group,
            "topic": topic,
            "state": str(description.state).split(".")[-1],
            "members": len(description.members),
            "lag": total_lag,
            "partitions": sorted(partitions, key=lambda p: p["partition"]),
        }

    def report(self) -> dict:
        with self._lock:
            return self._report()

    def _report(self) -> dict:
        if self._cache and time.time() - self._cache[0] < CACHE_SECONDS:
            return self._cache[1]
        metadata = self.admin.list_topics(timeout=5)
        groups = []
        for group, topic in CONSUMER_GROUPS.items():
            try:
                groups.append(self._group_report(metadata, group, topic))
            except Exception as exc:
                groups.append({"group": group, "topic": topic, "state": "UNKNOWN", "members": 0, "lag": None, "error": str(exc)})

        dead_letters = {}
        for topic in TOPICS_WITH_RETRY:
            dlq = dlq_topic(topic)
            dead_letters[dlq] = sum(
                high - low for low, high in (self._high_low(dlq, p) for p in self._partitions(metadata, dlq))
            )

        report = {
            "brokers": len(metadata.brokers),
            "topics": sorted(t for t in metadata.topics if not t.startswith("_")),
            "consumer_groups": groups,
            "dead_letters": dead_letters,
            "generated_at": int(time.time() * 1000),
        }
        self._cache = (time.time(), report)
        return report
