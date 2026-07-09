"""Ultra-Fast FastAPI Scoring Engine (Phase 4).

Serves the `/api/v1/score` endpoint to receive transactions, fetch features from Redis,
run the ONNX model, and apply business rules.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import numpy as np
import onnxruntime as ort
import redis.asyncio as redis
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, Depends, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from simpleeval import simple_eval
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fds.api")

# --- Configuration ---
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
ONNX_MODEL_PATH = os.environ.get("ONNX_MODEL_PATH", "models/fraud_xgb.onnx")

# From Phase 3 training
FEATURE_COLUMNS = [
    "tx_count_30d",
    "amt_mean_30d",
    "amt_std_30d",
    "amt_max_30d",
    "amt_min_30d",
    "distinct_mcc_30d",
    "distinct_merchant_30d",
    "distinct_country_30d",
    "pct_ecommerce",
    "pct_card_present",
    "pct_mobile",
    "avg_seconds_between_tx",
]


# --- Async SingleFlight ---
class AsyncSingleFlight:
    """Prevents cache stampedes by ensuring only one concurrent request per key."""
    def __init__(self):
        self._futures: dict[str, asyncio.Future] = {}

    async def do(self, key: str, func, *args, **kwargs) -> Any:
        if key in self._futures:
            return await self._futures[key]
        
        fut = asyncio.Future()
        self._futures[key] = fut
        
        try:
            result = await func(*args, **kwargs)
            fut.set_result(result)
            return result
        except Exception as e:
            fut.set_exception(e)
            raise
        finally:
            self._futures.pop(key, None)

single_flight = AsyncSingleFlight()


# --- Models ---
class TransactionInput(BaseModel):
    tx_id: str
    user_id: str
    amount_minor: int
    currency: str
    merchant_id: str
    mcc: str
    channel: str
    country: str
    ts_ms: int


class ScoreResponse(BaseModel):
    tx_id: str
    action: str  # APPROVE, REVIEW, DECLINE
    score: float
    rules_triggered: List[str]
    latency_ms: float


# --- Globals for Metrics ---
_redis_pool = None
_ort_session = None

# Global state for WebSocket dashboard
_metrics_state = {
    "total_tx_count": 0,
    "last_tx_count": 0,
    "total_latency_ms": 0.0,
    "latency_count": 0,
    "total_fraud_count": 0,
    "recent_txs": [],      # List of dicts
    "recent_alerts": [],   # List of dicts
}
_metrics_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis_pool, _ort_session
    
    # 1. Initialize Redis
    logger.info("Connecting to Redis: %s", REDIS_URL)
    _redis_pool = redis.from_url(REDIS_URL, decode_responses=True)
    await _redis_pool.ping()
    
    # 2. Initialize ONNX Runtime
    logger.info("Loading ONNX model: %s", ONNX_MODEL_PATH)
    if Path(ONNX_MODEL_PATH).exists():
        # Prefer CUDA if available, fallback to CPU
        providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        _ort_session = ort.InferenceSession(ONNX_MODEL_PATH, providers=providers)
    else:
        logger.warning("ONNX model not found at %s. Mocking inference for tests.", ONNX_MODEL_PATH)
        
    yield
    
    # Cleanup
    await _redis_pool.aclose()


app = FastAPI(title="FDS Scoring API", lifespan=lifespan)

# Mount static files for the dashboard
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Dependencies ---
def get_redis_client() -> redis.Redis:
    return _redis_pool

def get_model_session():
    return _ort_session


# --- Core Logic ---
async def _fetch_user_features(user_id: str, r: redis.Redis) -> dict:
    """Fetch real-time and offline features from Redis using Pipeline."""
    rt_key = f"feat:user:{user_id}"
    off_key = f"offline:user:{user_id}"
    
    async with r.pipeline(transaction=False) as pipe:
        pipe.hgetall(rt_key)
        pipe.hgetall(off_key)
        results = await pipe.execute()
    
    rt_feats, off_feats = results[0], results[1]
    
    # Merge dictionaries
    merged = {}
    if off_feats:
        merged.update(off_feats)
    if rt_feats:
        merged.update(rt_feats)
        
    return merged


def _evaluate_rules(tx: TransactionInput, score: float, feats: dict) -> tuple[str, list[str]]:
    """Evaluate business rules against transaction context, features, and ML score."""
    action = "APPROVE"
    rules_triggered = []
    
    # Context for rule engine
    context = tx.model_dump()
    context["score"] = score
    
    # Convert feature values to numeric safely
    for k, v in feats.items():
        try:
            context[k] = float(v)
        except (ValueError, TypeError):
            context[k] = 0.0
    
    # Defaults in case features are missing
    context.setdefault("tx_count_10m", 0.0)
    context.setdefault("tx_count_30d", 0.0)
    context.setdefault("amt_sum_1h", 0.0)
    
    # Hard rules (Deterministic DECLINE)
    hard_rules = {
        "Amount exceeds maximum": "amount_minor > 1000000",  # $10k limit
        "High fraud probability": "score > 0.90",
        "Velocity spike (Card Testing)": "tx_count_10m > 5", # Caught card testing!
        "New Account High Velocity": "tx_count_30d <= 5 and tx_count_10m >= 3", # Blocks GAN Attackers
    }
    
    for rule_name, expression in hard_rules.items():
        if simple_eval(expression, names=context):
            rules_triggered.append(rule_name)
            action = "DECLINE"
            
    # Soft rules (REVIEW)
    if action != "DECLINE":
        soft_rules = {
            "Suspiciously high score": "score > 0.70",
            "International high amount": "country != 'US' and amount_minor > 50000",
            "Smurfing detected": "amt_sum_1h > 4000000", # $40k total in 1 hour
            "New Account Early Activity": "tx_count_30d <= 5 and tx_count_10m >= 2", # Force review for new users
        }
        for rule_name, expression in soft_rules.items():
            if simple_eval(expression, names=context):
                rules_triggered.append(rule_name)
                action = "REVIEW"
                
    return action, rules_triggered


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/api/v1/score", response_model=ScoreResponse)
async def score_transaction(
    tx: TransactionInput,
    r: redis.Redis = Depends(get_redis_client),
    ort_sess = Depends(get_model_session)
):
    start_time = time.perf_counter()
    
    # Security Patch: Immediate state update to prevent direct API bypass
    rt_key = f"feat:user:{tx.user_id}"
    await r.hincrby(rt_key, "tx_count_10m", 1)
    await r.hincrby(rt_key, "tx_count_30d", 1)
    await r.expire(rt_key, 600)  # 10 mins TTL
    
    # 1. Fetch Features (with SingleFlight)
    feats = await single_flight.do(tx.user_id, _fetch_user_features, tx.user_id, r)
    
    # 2. Prepare Vector for ONNX
    vector = []
    for col in FEATURE_COLUMNS:
        val = feats.get(col, 0.0)
        try:
            vector.append(float(val))
        except (ValueError, TypeError):
            vector.append(0.0)
            
    # 3. Model Inference (Sync, fast enough to not block event loop)
    score = 0.0
    if ort_sess:
        x_tensor = np.array([vector], dtype=np.float32)
        input_name = ort_sess.get_inputs()[0].name
        
        result = ort_sess.run(None, {input_name: x_tensor})
        probs = result[1][0]
        score = float(probs[1] if isinstance(probs, (list, dict, tuple)) or len(probs) > 1 else probs[0])
    
    # 4. Rules Engine
    action, rules_triggered = _evaluate_rules(tx, score, feats)
    
    latency_ms = (time.perf_counter() - start_time) * 1000
    
    # --- Update Metrics for Dashboard ---
    import datetime
    formatted_amount = f"${tx.amount_minor / 100:,.2f}"
    time_str = datetime.datetime.now().strftime("%H:%M:%S")
    
    async with _metrics_lock:
        _metrics_state["total_tx_count"] += 1
        _metrics_state["total_latency_ms"] += latency_ms
        _metrics_state["latency_count"] += 1
        
        # Add to recent tx stream
        _metrics_state["recent_txs"].insert(0, {
            "tx_id": tx.tx_id,
            "user_id": tx.user_id,
            "amount": formatted_amount,
            "merchant_id": tx.merchant_id,
            "time": time_str
        })
        if len(_metrics_state["recent_txs"]) > 10:
            _metrics_state["recent_txs"].pop()
            
        # Add to alerts if declined/review
        if action != "APPROVE":
            _metrics_state["total_fraud_count"] += 1
            rule_str = ", ".join(rules_triggered) if rules_triggered else "Model Score"
            _metrics_state["recent_alerts"].insert(0, {
                "id": tx.tx_id,
                "amount": formatted_amount,
                "rule": rule_str,
                "type": action.lower(),
                "time": time_str
            })
            if len(_metrics_state["recent_alerts"]) > 50:
                _metrics_state["recent_alerts"].pop()
    # -------------------------------------
    
    return ScoreResponse(
        tx_id=tx.tx_id,
        action=action,
        score=score,
        rules_triggered=rules_triggered,
        latency_ms=round(latency_ms, 2)
    )

# --- WebSocket Metrics Stream ---
@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            # Calculate TPS and average latency for the past second
            async with _metrics_lock:
                current_total = _metrics_state["total_tx_count"]
                current_fraud = _metrics_state.get("total_fraud_count", 0)
                
                tps = current_total - _metrics_state["last_tx_count"]
                _metrics_state["last_tx_count"] = current_total
                
                lat_count = _metrics_state["latency_count"]
                avg_lat = (_metrics_state["total_latency_ms"] / lat_count) if lat_count > 0 else 0.0
                
                # Reset latency counters for next second
                _metrics_state["total_latency_ms"] = 0.0
                _metrics_state["latency_count"] = 0
                
                # Calculate Fraud Rate
                fraud_rate = (current_fraud / current_total * 100) if current_total > 0 else 0.0
                
                payload = {
                    "tps": tps,
                    "latency": round(avg_lat, 2),
                    "fraud_rate": round(fraud_rate, 2),
                    "recent_txs": _metrics_state["recent_txs"],
                    "recent_alerts": _metrics_state["recent_alerts"],
                }
            
            await websocket.send_json(payload)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass

# Mount Dashboard UI last so it serves static files on /dashboard
import pathlib
dashboard_dir = pathlib.Path(__file__).parent.parent.parent / "dashboard_ui"
app.mount("/dashboard", StaticFiles(directory=str(dashboard_dir), html=True), name="dashboard")
