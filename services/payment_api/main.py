import uuid
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Header, HTTPException, Response, status
from prometheus_client import make_asgi_app
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from common.config import get_settings
from common.kafka import now_ms
from common.logging_setup import setup_logging
from common.metrics import TXN_ACCEPTED
from common.topics import RAW

settings = get_settings()
setup_logging(settings.log_level)
pool = ConnectionPool(settings.database_url, min_size=2, max_size=20, open=False)


class TransactionIn(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    amount: float = Field(gt=0, le=10_000_000)
    currency: str = "INR"
    payee_id: str = Field(min_length=1, max_length=64)
    payee_type: Literal["P2P", "MERCHANT"]
    merchant_category: str
    channel: str = "UPI"
    device_id: str
    ip_address: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    city: str
    account_created_at: int
    timestamp: int | None = None
    ground_truth: str | None = None


class TransactionAccepted(BaseModel):
    txn_id: str
    status: Literal["ACCEPTED", "DUPLICATE"]


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True, timeout=30)
    yield
    pool.close()


app = FastAPI(title="FraudShield Payment API", version="1.0.0", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health() -> dict:
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/v1/transactions", status_code=status.HTTP_202_ACCEPTED, response_model=TransactionAccepted)
def create_transaction(
    body: TransactionIn,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> TransactionAccepted:
    key = idempotency_key or str(uuid.uuid4())
    txn_id = str(uuid.uuid4())
    event = body.model_dump()
    event["txn_id"] = txn_id
    event["timestamp"] = event["timestamp"] or now_ms()

    with pool.connection() as conn, conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO transactions (txn_id, idempotency_key, user_id, amount, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING txn_id
            """,
            (txn_id, key, body.user_id, body.amount, Jsonb(event)),
        ).fetchone()
        if inserted is None:
            existing = conn.execute(
                "SELECT txn_id FROM transactions WHERE idempotency_key = %s", (key,)
            ).fetchone()
            TXN_ACCEPTED.labels("duplicate").inc()
            response.status_code = status.HTTP_200_OK
            return TransactionAccepted(txn_id=existing[0], status="DUPLICATE")
        conn.execute(
            "INSERT INTO outbox (aggregate_id, topic, msg_key, payload) VALUES (%s, %s, %s, %s)",
            (txn_id, RAW, body.user_id, Jsonb(event)),
        )
    TXN_ACCEPTED.labels("accepted").inc()
    return TransactionAccepted(txn_id=txn_id, status="ACCEPTED")


@app.get("/v1/transactions/{txn_id}")
def get_transaction(txn_id: str) -> dict:
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT t.txn_id, t.payload, d.decision, d.risk_score, d.reasons, d.latency_ms
            FROM transactions t LEFT JOIN decisions d ON d.txn_id = t.txn_id
            WHERE t.txn_id = %s
            """,
            (txn_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return {
        "txn_id": row[0],
        "transaction": row[1],
        "decision": row[2] or "PENDING",
        "risk_score": row[3],
        "reasons": row[4] or [],
        "latency_ms": row[5],
    }
