import asyncio
import logging
import random
import signal
import threading
import uuid

import httpx

from common.config import get_settings
from common.kafka import Serde, instance_id, make_consumer, now_ms
from common.logging_setup import setup_logging
from common.metrics import SIM_SENT, serve_metrics
from common.simulation import FRAUD_PATTERNS, TransactionFactory
from common.topics import SIM_CONTROL

log = logging.getLogger("simulator")

TICK_SECONDS = 0.1
MAX_IN_FLIGHT = 200
LIVE_ATTACK_TIME_SCALE = 0.3
AVERAGE_FRAUD_SEQUENCE = 5


class Simulator:
    def __init__(self):
        self.settings = get_settings()
        self.factory = TransactionFactory(self.settings.sim_users, self.settings.sim_seed, reference_ms=now_ms())
        self.rng = random.Random(self.settings.sim_seed + 1)
        self.commands: asyncio.Queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(MAX_IN_FLIGHT)
        self.stopping = asyncio.Event()
        self.tasks: set[asyncio.Task] = set()

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def send(self, client: httpx.AsyncClient, txn: dict, kind: str) -> None:
        payload = {k: v for k, v in txn.items() if k != "txn_id"}
        payload["timestamp"] = now_ms()
        headers = {"Idempotency-Key": str(uuid.uuid4())}
        async with self.semaphore:
            for attempt in range(4):
                try:
                    response = await client.post("/v1/transactions", json=payload, headers=headers)
                    if response.status_code < 500:
                        SIM_SENT.labels(kind).inc()
                        return
                except httpx.HTTPError as exc:
                    log.debug("send failed: %s", exc)
                await asyncio.sleep(0.2 * 2**attempt)
        SIM_SENT.labels("failed").inc()

    async def send_later(self, client: httpx.AsyncClient, delay: float, txn: dict, kind: str) -> None:
        await asyncio.sleep(max(delay, 0.0))
        await self.send(client, txn, kind)

    def launch_attack(self, client: httpx.AsyncClient, pattern: str, time_scale: float) -> None:
        user = self.factory.random_user()
        start = now_ms()
        sequence = self.factory.attack(pattern, user, start, fast=time_scale < 1.0)
        for txn in sequence:
            delay = (txn["timestamp"] - start) / 1000.0 * time_scale
            kind = "fraud" if txn["ground_truth"] else "normal"
            self._spawn(self.send_later(client, delay, txn, kind))
        log.info("launched %s attack on %s with %s transactions", pattern, user.user_id, len(sequence))

    async def traffic(self, client: httpx.AsyncClient) -> None:
        per_tick = self.settings.sim_tps * TICK_SECONDS
        attack_probability = self.settings.sim_fraud_rate / AVERAGE_FRAUD_SEQUENCE
        carry = 0.0
        while not self.stopping.is_set():
            carry += per_tick
            count, carry = int(carry), carry - int(carry)
            for _ in range(count):
                if self.rng.random() < attack_probability:
                    self.launch_attack(client, self.rng.choice(FRAUD_PATTERNS), 1.0)
                else:
                    txn = self.factory.normal(self.factory.random_user(), now_ms())
                    self._spawn(self.send(client, txn, "normal"))
            await asyncio.sleep(TICK_SECONDS)

    async def command_loop(self, client: httpx.AsyncClient) -> None:
        while not self.stopping.is_set():
            command = await self.commands.get()
            if command.get("command") != "attack" or command.get("pattern") not in FRAUD_PATTERNS:
                log.warning("ignoring command %s", command)
                continue
            for _ in range(max(command.get("count", 1), 1)):
                self.launch_attack(client, command["pattern"], LIVE_ATTACK_TIME_SCALE)
                await asyncio.sleep(0.2)

    def control_consumer(self, loop: asyncio.AbstractEventLoop) -> None:
        serde = Serde(self.settings)
        consumer = make_consumer(
            self.settings,
            f"simulator-{instance_id()}",
            **{"auto.offset.reset": "latest", "enable.auto.commit": True},
        )
        consumer.subscribe([SIM_CONTROL])
        try:
            while not self.stopping.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                try:
                    command = serde.deserialize(SIM_CONTROL, msg.value())
                except Exception as exc:
                    log.warning("bad control message: %s", exc)
                    continue
                loop.call_soon_threadsafe(self.commands.put_nowait, command)
        finally:
            consumer.close()

    async def wait_for_api(self, client: httpx.AsyncClient) -> None:
        while True:
            try:
                if (await client.get("/health")).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            log.info("waiting for payment api at %s", self.settings.payment_api_url)
            await asyncio.sleep(2)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stopping.set)
            except NotImplementedError:
                pass
        serve_metrics(self.settings.metrics_port)
        limits = httpx.Limits(max_connections=MAX_IN_FLIGHT, max_keepalive_connections=50)
        async with httpx.AsyncClient(base_url=self.settings.payment_api_url, timeout=5.0, limits=limits) as client:
            await self.wait_for_api(client)
            threading.Thread(target=self.control_consumer, args=(loop,), daemon=True).start()
            log.info("simulating %s tps across %s users", self.settings.sim_tps, self.settings.sim_users)
            workers = [asyncio.create_task(self.traffic(client)), asyncio.create_task(self.command_loop(client))]
            await self.stopping.wait()
            for worker in workers:
                worker.cancel()


def main() -> None:
    setup_logging(get_settings().log_level)
    asyncio.run(Simulator().run())


if __name__ == "__main__":
    main()
