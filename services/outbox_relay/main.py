import logging
import signal
import time

import psycopg

from common.config import get_settings
from common.kafka import EventPublisher
from common.logging_setup import setup_logging
from common.metrics import OUTBOX_PENDING, OUTBOX_PUBLISHED, serve_metrics

log = logging.getLogger("outbox-relay")

BATCH_SIZE = 500
IDLE_SLEEP_SECONDS = 0.05
CLEANUP_EVERY_SECONDS = 60
GAUGE_EVERY_SECONDS = 5
RETAIN_PUBLISHED = "1 hour"


class OutboxRelay:
    def __init__(self):
        self.settings = get_settings()
        self.publisher = EventPublisher(self.settings, "outbox-relay")
        self.running = True
        self.last_cleanup = 0.0
        self.last_gauge = 0.0

    def stop(self, *_) -> None:
        self.running = False

    def connect(self) -> psycopg.Connection:
        while True:
            try:
                return psycopg.connect(self.settings.database_url, autocommit=True)
            except psycopg.OperationalError as exc:
                log.warning("database unavailable: %s", exc)
                time.sleep(2)

    def publish_batch(self, conn: psycopg.Connection) -> int:
        with conn.transaction():
            rows = conn.execute(
                """
                SELECT id, topic, msg_key, payload FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (BATCH_SIZE,),
            ).fetchall()
            if not rows:
                return 0
            for _, topic, key, payload in rows:
                self.publisher.publish(topic, key, payload)
            failures = self.publisher.flush(15)
            if failures:
                raise RuntimeError(f"kafka delivery failed: {failures[:3]}")
            conn.execute(
                "UPDATE outbox SET published_at = now() WHERE id = ANY(%s)", ([r[0] for r in rows],)
            )
        OUTBOX_PUBLISHED.inc(len(rows))
        return len(rows)

    def housekeeping(self, conn: psycopg.Connection) -> None:
        now = time.time()
        if now - self.last_gauge >= GAUGE_EVERY_SECONDS:
            self.last_gauge = now
            with conn.transaction():
                pending = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
            OUTBOX_PENDING.set(pending)
        if now - self.last_cleanup >= CLEANUP_EVERY_SECONDS:
            self.last_cleanup = now
            with conn.transaction():
                conn.execute(
                    f"DELETE FROM outbox WHERE published_at < now() - interval '{RETAIN_PUBLISHED}'"
                )

    def run(self) -> None:
        serve_metrics(self.settings.metrics_port)
        conn = self.connect()
        log.info("outbox relay started")
        while self.running:
            try:
                published = self.publish_batch(conn)
                self.housekeeping(conn)
                if published == 0:
                    time.sleep(IDLE_SLEEP_SECONDS)
            except psycopg.OperationalError as exc:
                log.warning("lost database connection: %s", exc)
                conn = self.connect()
            except RuntimeError as exc:
                log.error("%s, batch will be retried", exc)
                time.sleep(1)
        conn.close()
        self.publisher.flush(10)


def main() -> None:
    setup_logging(get_settings().log_level)
    relay = OutboxRelay()
    signal.signal(signal.SIGTERM, relay.stop)
    signal.signal(signal.SIGINT, relay.stop)
    relay.run()


if __name__ == "__main__":
    main()
