import logging
import signal
import sys
import threading
import time
from collections.abc import Callable

from confluent_kafka import OFFSET_BEGINNING, KafkaException, Producer

from common.config import Settings
from common.kafka import Serde, instance_id, is_fatal, make_consumer, producer_config
from common.metrics import (
    BATCH_SIZE,
    DEAD_LETTERS,
    EVENTS_PROCESSED,
    PROCESSING_SECONDS,
    RETRIES,
    TXN_ABORTS,
    serve_metrics,
)
from common.topics import TOPICS_WITH_RETRY, dlq_topic, retry_topic, source_topic

log = logging.getLogger(__name__)

Output = tuple[str, str, dict]
Handler = Callable[[dict], list[Output]]

MAX_RETRY_WAIT_SECONDS = 30.0
BATCH_WAIT_SECONDS = 0.05


class StreamProcessor:
    def __init__(
        self,
        service: str,
        group_id: str,
        input_topic: str,
        handler: Handler,
        settings: Settings,
        serde: Serde | None = None,
        batch_size: int = 200,
    ):
        if input_topic not in TOPICS_WITH_RETRY:
            raise ValueError(f"{input_topic} has no retry/DLQ topics configured")
        self.service = service
        self.input_topic = input_topic
        self.handler = handler
        self.settings = settings
        self.batch_size = batch_size
        self.serde = serde or Serde(settings)
        self.topics = [input_topic, retry_topic(input_topic)]
        self.consumer = make_consumer(settings, group_id)
        self.producer = Producer(
            producer_config(settings, f"{service}-producer", transactional_id=f"{service}-{instance_id()}")
        )
        self._running = threading.Event()
        self._running.set()

    def stop(self) -> None:
        self._running.clear()

    def run(self) -> None:
        self.producer.init_transactions(60)
        self.consumer.subscribe(self.topics)
        log.info("%s consuming %s", self.service, self.topics)
        try:
            while self._running.is_set():
                messages = self.consumer.consume(self.batch_size, timeout=BATCH_WAIT_SECONDS)
                if messages:
                    self._process_batch(messages)
        finally:
            self.consumer.close()
            log.info("%s stopped", self.service)

    def _process_batch(self, messages: list) -> None:
        self.producer.begin_transaction()
        try:
            processed = 0
            for msg in messages:
                if msg.error():
                    if is_fatal(msg.error()):
                        raise KafkaException(msg.error())
                    log.warning("consumer error %s", msg.error())
                    continue
                self._process(msg)
                processed += 1
            assignment = self.consumer.assignment()
            positions = [tp for tp in self.consumer.position(assignment) if tp.offset >= 0] if assignment else []
            if positions:
                self.producer.send_offsets_to_transaction(positions, self.consumer.consumer_group_metadata(), 30)
            self.producer.commit_transaction(30)
            BATCH_SIZE.labels(self.service).observe(processed)
        except KafkaException as exc:
            error = exc.args[0]
            if not error.txn_requires_abort():
                raise
            log.warning("aborting transaction: %s", error)
            TXN_ABORTS.labels(self.service).inc()
            self.producer.abort_transaction(30)
            self._rewind()

    def _rewind(self) -> None:
        assignment = self.consumer.assignment()
        if not assignment:
            return
        for tp in self.consumer.committed(assignment, timeout=10):
            if tp.offset < 0:
                tp.offset = OFFSET_BEGINNING
            self.consumer.seek(tp)

    def _process(self, msg) -> None:
        topic = msg.topic()
        origin = source_topic(topic)
        headers = {k: v for k, v in (msg.headers() or [])}
        attempt = int(headers.get("x-attempt", b"0").decode())
        if topic != origin:
            self._wait_until(float(headers.get("x-not-before", b"0").decode()))

        started = time.perf_counter()
        try:
            event = self.serde.deserialize(origin, msg.value())
        except Exception as exc:
            log.error("poison message on %s offset %s: %s", topic, msg.offset(), exc)
            self._route(msg, origin, attempt, exc, dead=True)
            return

        try:
            outputs = self.handler(event) or []
            encoded = [(t, k.encode(), self.serde.serialize(t, v)) for t, k, v in outputs]
        except Exception as exc:
            next_attempt = attempt + 1
            dead = next_attempt > self.settings.max_retries
            log.warning("handler failed attempt=%s dead=%s error=%s", next_attempt, dead, exc)
            self._route(msg, origin, next_attempt, exc, dead=dead)
            return

        for out_topic, key, value in encoded:
            self._produce(out_topic, key, value)
        EVENTS_PROCESSED.labels(self.service, origin).inc()
        PROCESSING_SECONDS.labels(self.service).observe(time.perf_counter() - started)

    def _route(self, msg, origin: str, attempt: int, exc: Exception, dead: bool) -> None:
        target = dlq_topic(origin) if dead else retry_topic(origin)
        headers = [
            ("x-source-topic", origin),
            ("x-attempt", str(attempt)),
            ("x-error-type", type(exc).__name__),
            ("x-error", str(exc)[:500]),
            ("x-failed-service", self.service),
            ("x-original-partition", str(msg.partition())),
            ("x-original-offset", str(msg.offset())),
        ]
        if dead:
            DEAD_LETTERS.labels(self.service, origin).inc()
        else:
            delay = self.settings.retry_backoff_seconds * (2 ** (attempt - 1))
            headers.append(("x-not-before", str(time.time() + delay)))
            RETRIES.labels(self.service, origin).inc()
        self._produce(target, msg.key(), msg.value(), headers)

    def _produce(self, topic: str, key: bytes | None, value: bytes, headers: list | None = None) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, headers=headers)
                return
            except BufferError:
                self.producer.poll(0.1)

    def _wait_until(self, not_before: float) -> None:
        delay = not_before - time.time()
        if delay > 0:
            time.sleep(min(delay, MAX_RETRY_WAIT_SECONDS))


def run_processors(processors: list[StreamProcessor], metrics_port: int) -> None:
    serve_metrics(metrics_port)

    def shutdown(*_):
        for p in processors:
            p.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    crashed = threading.Event()

    def guarded(processor: StreamProcessor) -> None:
        try:
            processor.run()
        except Exception:
            log.exception("%s crashed", processor.service)
            crashed.set()
            shutdown()

    threads = [threading.Thread(target=guarded, args=(p,), name=p.service, daemon=True) for p in processors]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=1.0)
    if crashed.is_set():
        sys.exit(1)
