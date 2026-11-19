import logging
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from common.config import get_settings
from common.logging_setup import setup_logging
from common.topics import all_topic_specs

log = logging.getLogger("topic-init")


def wait_for_broker(admin: AdminClient, timeout: float = 120) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception as exc:
            if time.time() > deadline:
                raise
            log.info("waiting for kafka: %s", exc)
            time.sleep(2)


def create_topics(replication_factor: int = 1) -> int:
    settings = get_settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    wait_for_broker(admin)
    existing = set(admin.list_topics(timeout=10).topics)
    specs = all_topic_specs()
    missing = [
        NewTopic(name, num_partitions=spec["partitions"], replication_factor=replication_factor, config=spec["config"])
        for name, spec in specs.items()
        if name not in existing
    ]
    if not missing:
        log.info("all %s topics already exist", len(specs))
        return 0
    failures = 0
    for name, future in admin.create_topics(missing).items():
        try:
            future.result()
            log.info("created topic %s", name)
        except Exception as exc:
            failures += 1
            log.error("failed to create %s: %s", name, exc)
    return failures


def main() -> None:
    setup_logging(get_settings().log_level)
    sys.exit(1 if create_topics() else 0)


if __name__ == "__main__":
    main()
