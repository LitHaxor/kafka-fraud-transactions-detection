"""Live web dashboard for the demo bank.

Consumes bank.transactions and bank.alerts in background threads, keeps a
small in-memory view (balances, recent activity, fraud alerts) and pushes
every event to connected browsers over a websocket.

    python -m bank.dashboard      ->  http://127.0.0.1:8000
"""

import asyncio
import json
import threading
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from bank.seed import load
from config import settings

STATIC = Path(__file__).parent / "static"
# Deposits credit the account; everything else debits it.
CREDIT_TYPES = {"DEPOSIT"}


class Hub:
    """Shared state between the Kafka threads and the websocket clients."""

    def __init__(self):
        customers, accounts = load()
        self.customers = {c["customer_id"]: c for c in customers}
        self.accounts = {a["account_id"]: a for a in accounts}
        self.balances = {a["account_id"]: a["opening_balance"] for a in accounts}

        self.transactions = deque(maxlen=60)
        self.alerts = deque(maxlen=40)
        self.stats = {"txns": 0, "alerts": 0, "volume": 0.0}

        self.clients: set[WebSocket] = set()
        self.queue: asyncio.Queue | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.lock = threading.Lock()

    def _publish(self, message: dict):
        """Called from a Kafka thread; hands the message to the event loop."""
        if self.loop and self.queue:
            self.loop.call_soon_threadsafe(self.queue.put_nowait, message)

    def on_transaction(self, txn: dict):
        with self.lock:
            acct = txn.get("account_id")
            amount = float(txn.get("amount", 0))
            if acct in self.balances:
                delta = amount if txn.get("txn_type") in CREDIT_TYPES else -amount
                self.balances[acct] = round(self.balances[acct] + delta, 2)
            cust = self.customers.get(txn.get("customer_id"), {})
            txn = {**txn, "customer_name": cust.get("name", "unknown"),
                   "balance": self.balances.get(acct)}
            self.transactions.appendleft(txn)
            self.stats["txns"] += 1
            self.stats["volume"] = round(self.stats["volume"] + amount, 2)
            stats = dict(self.stats)
        self._publish({"type": "txn", "data": txn, "stats": stats,
                       "balance": {"account_id": acct, "value": self.balances.get(acct)}})

    def on_alert(self, alert: dict):
        with self.lock:
            cust = self.customers.get(alert.get("customer_id"), {})
            alert = {**alert, "customer_name": cust.get("name", "unknown")}
            self.alerts.appendleft(alert)
            self.stats["alerts"] += 1
            stats = dict(self.stats)
        self._publish({"type": "alert", "data": alert, "stats": stats})

    def snapshot(self):
        with self.lock:
            return {
                "type": "snapshot",
                "stats": dict(self.stats),
                "transactions": list(self.transactions),
                "alerts": list(self.alerts),
                "accounts": [
                    {**a, "balance": self.balances[a["account_id"]],
                     "customer_name": self.customers[a["customer_id"]]["name"],
                     "risk_band": self.customers[a["customer_id"]]["risk_band"]}
                    for a in self.accounts.values()
                ],
            }


hub = Hub()


def consume(topic: str, group_suffix: str, handler, stop: threading.Event):
    """Run one Kafka consumer until stop is set."""
    consumer = Consumer({
        'bootstrap.servers': settings.KAFKA_BROKER,
        'group.id': f"{settings.BANK_GROUP_ID}-{group_suffix}",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([topic])
    try:
        while not stop.is_set():
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[{topic}] {msg.error()}")
                continue
            try:
                handler(json.loads(msg.value()))
            except Exception as exc:  # a bad record must not kill the feed
                print(f"[{topic}] bad record: {exc}")
    finally:
        consumer.close()


async def broadcaster():
    """Fan queued messages out to every connected browser."""
    assert hub.queue is not None
    while True:
        message = await hub.queue.get()
        for ws in list(hub.clients):
            try:
                await ws.send_json(message)
            except Exception:
                hub.clients.discard(ws)


@asynccontextmanager
async def lifespan(app: FastAPI):
    hub.loop = asyncio.get_running_loop()
    hub.queue = asyncio.Queue(maxsize=2000)
    stop = threading.Event()
    threads = [
        threading.Thread(target=consume, args=(settings.BANK_TXN_TOPIC, "txn",
                                               hub.on_transaction, stop), daemon=True),
        threading.Thread(target=consume, args=(settings.BANK_ALERTS_TOPIC, "alert",
                                               hub.on_alert, stop), daemon=True),
    ]
    for t in threads:
        t.start()
    task = asyncio.create_task(broadcaster())

    print(f"dashboard on http://{settings.DASHBOARD_HOST}:{settings.DASHBOARD_PORT}")
    print(f"consuming {settings.BANK_TXN_TOPIC} and {settings.BANK_ALERTS_TOPIC}")
    yield

    stop.set()
    task.cancel()
    for t in threads:
        t.join(timeout=3)


app = FastAPI(title="Demo Bank", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC / "index.html").read_text()


@app.get("/api/state")
async def state():
    return JSONResponse(hub.snapshot())


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    hub.clients.add(ws)
    await ws.send_json(hub.snapshot())
    try:
        while True:
            await ws.receive_text()  # clients don't send; this just detects close
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)


if __name__ == "__main__":
    uvicorn.run(app, host=settings.DASHBOARD_HOST, port=settings.DASHBOARD_PORT,
                log_level="warning")
