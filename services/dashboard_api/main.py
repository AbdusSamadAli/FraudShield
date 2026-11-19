import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from common.config import get_settings
from common.kafka import EventPublisher, now_ms
from common.logging_setup import setup_logging
from common.model import METADATA_FILE
from common.rules import RULES_BY_CODE
from common.simulation import FRAUD_PATTERNS
from common.topics import FEEDBACK, SIM_CONTROL
from services.dashboard_api.live import LiveHub
from services.dashboard_api.system import KafkaMonitor

settings = get_settings()
setup_logging(settings.log_level)
pool = ConnectionPool(settings.database_url, min_size=2, max_size=10, open=False, kwargs={"row_factory": dict_row})
hub = LiveHub(settings)
state: dict = {}

SUMMARY_COLUMNS = """
    txn_id, user_id, amount::float AS amount, city, payee_id, payee_type, category, device_id, decision,
    risk_score, ml_score, anomaly_score, rule_hits, reasons, ground_truth, latency_ms, event_ts,
    decided_at, status, label, labeled_by
"""


class FeedbackIn(BaseModel):
    label: Literal["FRAUD", "LEGIT"]
    analyst: str = Field(default="analyst", max_length=64)
    notes: str | None = Field(default=None, max_length=500)


class AttackIn(BaseModel):
    pattern: str
    count: int = Field(default=1, ge=1, le=20)


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True, timeout=30)
    state["publisher"] = EventPublisher(settings, "dashboard-api")
    state["monitor"] = KafkaMonitor(settings)
    hub.start(asyncio.get_running_loop())
    yield
    hub.stop()
    state["publisher"].flush(5)
    pool.close()


app = FastAPI(title="FraudShield Dashboard API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/metrics", make_asgi_app())


def query(sql: str, params=None) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def serialize(row: dict) -> dict:
    out = dict(row)
    for key in ("decided_at", "labeled_at"):
        if out.get(key) is not None:
            out[key] = int(out[key].timestamp() * 1000)
    return out


@app.get("/api/health")
def health() -> dict:
    query("SELECT 1 AS ok")
    return {"status": "ok"}


@app.get("/api/transactions/recent")
def recent_transactions(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    rows = query(f"SELECT {SUMMARY_COLUMNS} FROM decisions ORDER BY decided_at DESC LIMIT %s", (limit,))
    return [serialize(r) for r in rows]


@app.get("/api/alerts")
def list_alerts(
    status: Literal["OPEN", "RESOLVED", "ALL"] = "OPEN",
    decision: Literal["REVIEW", "BLOCK", "ALL"] = "ALL",
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
    clauses = ["decision <> 'APPROVE'"]
    params: list = []
    if status != "ALL":
        clauses.append("status = %s")
        params.append(status)
    if decision != "ALL":
        clauses.append("decision = %s")
        params.append(decision)
    params.append(limit)
    rows = query(
        f"SELECT {SUMMARY_COLUMNS} FROM decisions WHERE {' AND '.join(clauses)} ORDER BY decided_at DESC LIMIT %s",
        params,
    )
    return [serialize(r) for r in rows]


@app.get("/api/alerts/{txn_id}")
def alert_detail(txn_id: str) -> dict:
    rows = query(
        f"SELECT {SUMMARY_COLUMNS}, rule_score, features, model_version, labeled_at FROM decisions WHERE txn_id = %s",
        (txn_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="alert not found")
    detail = serialize(rows[0])
    detail["rules"] = [
        {"code": code, "description": RULES_BY_CODE[code].description, "score": RULES_BY_CODE[code].score}
        for code in detail["rule_hits"]
        if code in RULES_BY_CODE
    ]
    detail["user_history"] = [
        serialize(r)
        for r in query(
            f"SELECT {SUMMARY_COLUMNS} FROM decisions WHERE user_id = %s ORDER BY decided_at DESC LIMIT 10",
            (detail["user_id"],),
        )
    ]
    return detail


@app.post("/api/alerts/{txn_id}/feedback", status_code=202)
def submit_feedback(txn_id: str, body: FeedbackIn) -> dict:
    if not query("SELECT txn_id FROM decisions WHERE txn_id = %s", (txn_id,)):
        raise HTTPException(status_code=404, detail="alert not found")
    publisher: EventPublisher = state["publisher"]
    publisher.publish(
        FEEDBACK,
        txn_id,
        {"txn_id": txn_id, "label": body.label, "analyst": body.analyst, "notes": body.notes, "created_at": now_ms()},
    )
    failures = publisher.flush(5)
    if failures:
        raise HTTPException(status_code=503, detail="could not publish feedback")
    return {"status": "queued", "txn_id": txn_id, "label": body.label}


@app.get("/api/stats")
def stats(minutes: int = Query(default=15, ge=1, le=1440)) -> dict:
    window = (minutes,)
    since = "decided_at > now() - make_interval(mins => %s)"
    summary = query(
        f"""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE decision = 'APPROVE') AS approved,
               count(*) FILTER (WHERE decision = 'REVIEW') AS review,
               count(*) FILTER (WHERE decision = 'BLOCK') AS blocked,
               coalesce(sum(amount) FILTER (WHERE decision = 'BLOCK'), 0)::float AS blocked_amount,
               coalesce(avg(latency_ms), 0) AS avg_latency_ms,
               coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0) AS p95_latency_ms,
               count(*) FILTER (WHERE ground_truth IS NOT NULL AND decision <> 'APPROVE') AS true_positive,
               count(*) FILTER (WHERE ground_truth IS NOT NULL AND decision = 'APPROVE') AS false_negative,
               count(*) FILTER (WHERE ground_truth IS NULL AND decision <> 'APPROVE') AS false_positive,
               count(*) FILTER (WHERE decided_at > now() - interval '60 seconds') AS last_minute
        FROM decisions WHERE {since}
        """,
        window,
    )[0]
    tp, fn, fp = summary["true_positive"], summary["false_negative"], summary["false_positive"]
    summary["precision"] = round(tp / (tp + fp), 4) if tp + fp else None
    summary["recall"] = round(tp / (tp + fn), 4) if tp + fn else None
    summary["throughput_per_sec"] = round(summary["last_minute"] / 60.0, 2)

    timeseries = query(
        f"""
        SELECT extract(epoch FROM date_trunc('minute', decided_at))::bigint * 1000 AS minute,
               count(*) FILTER (WHERE decision = 'APPROVE') AS approved,
               count(*) FILTER (WHERE decision = 'REVIEW') AS review,
               count(*) FILTER (WHERE decision = 'BLOCK') AS blocked,
               avg(latency_ms) AS avg_latency_ms
        FROM decisions WHERE {since}
        GROUP BY 1 ORDER BY 1
        """,
        window,
    )
    rule_hits = query(
        f"""
        SELECT hit AS rule, count(*) AS hits
        FROM decisions, jsonb_array_elements_text(rule_hits) AS hit
        WHERE {since} GROUP BY hit ORDER BY hits DESC
        """,
        window,
    )
    patterns = query(
        f"""
        SELECT ground_truth AS pattern, count(*) AS total,
               count(*) FILTER (WHERE decision <> 'APPROVE') AS caught
        FROM decisions WHERE {since} AND ground_truth IS NOT NULL
        GROUP BY ground_truth ORDER BY total DESC
        """,
        window,
    )
    cities = query(
        f"""
        SELECT city, count(*) AS alerts FROM decisions
        WHERE {since} AND decision <> 'APPROVE'
        GROUP BY city ORDER BY alerts DESC LIMIT 8
        """,
        window,
    )
    labels = query(
        """
        SELECT label, count(*) AS total FROM decisions WHERE label IS NOT NULL GROUP BY label
        """
    )
    return {
        "minutes": minutes,
        "summary": summary,
        "timeseries": timeseries,
        "rule_hits": rule_hits,
        "patterns": patterns,
        "cities": cities,
        "labels": {r["label"]: r["total"] for r in labels},
    }


@app.get("/api/model")
def model_info() -> dict:
    path = Path(settings.model_dir) / METADATA_FILE
    if not path.exists():
        raise HTTPException(status_code=404, detail="model not trained yet")
    return json.loads(path.read_text())


@app.get("/api/system")
def system() -> dict:
    return state["monitor"].report()


@app.get("/api/simulator/patterns")
def attack_patterns() -> list[str]:
    return FRAUD_PATTERNS


@app.post("/api/simulator/attack", status_code=202)
def launch_attack(body: AttackIn) -> dict:
    if body.pattern not in FRAUD_PATTERNS:
        raise HTTPException(status_code=400, detail=f"pattern must be one of {FRAUD_PATTERNS}")
    publisher: EventPublisher = state["publisher"]
    publisher.publish(
        SIM_CONTROL,
        "attack",
        {"command": "attack", "pattern": body.pattern, "count": body.count, "issued_at": now_ms()},
    )
    publisher.flush(5)
    return {"status": "launched", "pattern": body.pattern, "count": body.count}


@app.websocket("/ws/live")
async def live(websocket: WebSocket) -> None:
    try:
        await hub.serve(websocket)
    except WebSocketDisconnect:
        pass
