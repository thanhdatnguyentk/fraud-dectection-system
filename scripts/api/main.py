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
from fastapi import FastAPI, Depends, Request
from pydantic import BaseModel, Field
from simpleeval import simple_eval

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


# --- Globals ---
_redis_pool = None
_ort_session = None


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


def _evaluate_rules(tx: TransactionInput, score: float) -> tuple[str, list[str]]:
    """Evaluate business rules against transaction context and ML score."""
    action = "APPROVE"
    rules_triggered = []
    
    # Context for rule engine
    context = tx.model_dump()
    context["score"] = score
    
    # Hard rules (Deterministic DECLINE)
    hard_rules = {
        "Amount exceeds maximum": "amount_minor > 1000000",  # $10k limit
        "High fraud probability": "score > 0.90",
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
        
        # We use run() directly since ONNX CPU inference is typically < 1ms
        result = ort_sess.run(None, {input_name: x_tensor})
        
        # Result shape varies by onnxmltools, typically probabilities are in result[1]
        probs = result[1][0]
        # prob of class 1
        score = float(probs[1] if isinstance(probs, (list, dict, tuple)) or len(probs) > 1 else probs[0])
    
    # 4. Rules Engine
    action, rules_triggered = _evaluate_rules(tx, score)
    
    latency_ms = (time.perf_counter() - start_time) * 1000
    
    return ScoreResponse(
        tx_id=tx.tx_id,
        action=action,
        score=score,
        rules_triggered=rules_triggered,
        latency_ms=round(latency_ms, 2)
    )

from pathlib import Path
