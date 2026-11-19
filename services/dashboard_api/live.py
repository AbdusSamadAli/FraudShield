import asyncio
import logging
import threading

from fastapi import WebSocket

from common.config import Settings
from common.kafka import Serde, instance_id, make_consumer
from common.topics import DECISIONS

log = logging.getLogger("dashboard-live")

QUEUE_SIZE = 1000


def compact(decision: dict) -> dict:
    txn = decision["transaction"]
    return {
        "txn_id": txn["txn_id"],
        "user_id": txn["user_id"],
        "amount": txn["amount"],
        "city": txn["city"],
        "payee_id": txn["payee_id"],
        "payee_type": txn["payee_type"],
        "category": txn["merchant_category"],
        "device_id": txn["device_id"],
        "ground_truth": txn.get("ground_truth"),
        "timestamp": txn["timestamp"],
        "decision": decision["decision"],
        "risk_score": decision["risk_score"],
        "ml_score": decision["ml_score"],
        "anomaly_score": decision["anomaly_score"],
        "rule_hits": decision["rule_hits"],
        "reasons": [r["text"] for r in decision["reasons"]],
        "latency_ms": decision["latency_ms"],
        "decided_at": decision["decided_at"],
    }


class LiveHub:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.clients: set[asyncio.Queue] = set()
        self.loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self._thread = threading.Thread(target=self._consume, name="live-consumer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _broadcast(self, item: dict) -> None:
        for queue in list(self.clients):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(item)

    def _consume(self) -> None:
        serde = Serde(self.settings)
        consumer = make_consumer(
            self.settings,
            f"dashboard-live-{instance_id()}",
            **{"auto.offset.reset": "latest", "enable.auto.commit": True},
        )
        consumer.subscribe([DECISIONS])
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                try:
                    item = compact(serde.deserialize(DECISIONS, msg.value()))
                except Exception as exc:
                    log.warning("skipping undecodable decision: %s", exc)
                    continue
                if self.clients and self.loop:
                    self.loop.call_soon_threadsafe(self._broadcast, item)
        finally:
            consumer.close()

    async def serve(self, websocket: WebSocket) -> None:
        await websocket.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.clients.add(queue)
        try:
            while True:
                batch = [await queue.get()]
                while not queue.empty() and len(batch) < 100:
                    batch.append(queue.get_nowait())
                await websocket.send_json(batch)
        finally:
            self.clients.discard(queue)
