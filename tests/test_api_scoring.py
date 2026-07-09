"""Unit tests for the FastAPI Scoring API (Phase 4)."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

# We will implement the app in scripts.api.main
from scripts.api.main import app, get_redis_client, get_model_session

# Use fakeredis for tests
import fakeredis

client = TestClient(app)

class DummySession:
    def __init__(self):
        self.inputs = [{"name": "input"}]
    
    def get_inputs(self):
        class DummyInput:
            name = "input"
        return [DummyInput()]
    
    def run(self, output_names, input_feed):
        # Fake output: [class_predictions, probabilities]
        # Return probability of fraud = 0.85
        return [None, [[0.15, 0.85]]]

@pytest.fixture
def override_dependencies():
    import asyncio
    fake_redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    
    async def populate():
        # Pre-populate some user features for testing
        user_id = "test_user_123"
        await fake_redis.hset(f"feat:user:{user_id}", mapping={
            "tx_count_10m": "5",
            "amt_sum_1h": "15000",
            "max_amt_1h": "5000",
            "distinct_mcc_1h": "2",
            "seconds_since_last_tx": "120"
        })
        
        # Populate offline features needed by model (mocked)
        await fake_redis.hset(f"offline:user:{user_id}", mapping={
            "tx_count_30d": "45",
            "amt_mean_30d": "2500",
            "amt_std_30d": "1000",
            "amt_max_30d": "8000",
            "amt_min_30d": "500",
            "distinct_mcc_30d": "5",
            "distinct_merchant_30d": "8",
            "distinct_country_30d": "1",
            "pct_ecommerce": "0.8",
            "pct_card_present": "0.2",
            "pct_mobile": "0.0",
            "avg_seconds_between_tx": "86400"
        })
    
    asyncio.run(populate())
    
    def override_get_redis():
        return fake_redis

    def override_get_model():
        return DummySession()
        
    app.dependency_overrides[get_redis_client] = override_get_redis
    app.dependency_overrides[get_model_session] = override_get_model
    
    yield fake_redis
    
    app.dependency_overrides = {}


class TestScoringAPI:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        
    def test_score_valid_transaction(self, override_dependencies):
        tx_data = {
            "tx_id": "tx_999",
            "user_id": "test_user_123",
            "amount_minor": 12000,
            "currency": "USD",
            "merchant_id": "merch_55",
            "mcc": "5411",
            "channel": "ecommerce",
            "country": "US",
            "ts_ms": 1700000000000
        }
        
        response = client.post("/api/v1/score", json=tx_data)
        assert response.status_code == 200
        data = response.json()
        
        assert "score" in data
        assert "action" in data
        assert data["action"] in ["APPROVE", "REVIEW", "DECLINE"]
        assert data["score"] == 0.85
        assert "latency_ms" in data

    def test_score_missing_features(self, override_dependencies):
        # A user not in Redis
        tx_data = {
            "tx_id": "tx_999",
            "user_id": "unknown_user",
            "amount_minor": 1000,
            "currency": "USD",
            "merchant_id": "merch_55",
            "mcc": "5411",
            "channel": "ecommerce",
            "country": "US",
            "ts_ms": 1700000000000
        }
        
        response = client.post("/api/v1/score", json=tx_data)
        assert response.status_code == 200
        data = response.json()
        assert "score" in data
        # Fallback handling should allow processing but maybe with 0s

    def test_hard_rule_decline(self, override_dependencies):
        tx_data = {
            "tx_id": "tx_999",
            "user_id": "test_user_123",
            "amount_minor": 10000000, # 10M cents = $100k, should trigger amount rule
            "currency": "USD",
            "merchant_id": "merch_55",
            "mcc": "5411",
            "channel": "ecommerce",
            "country": "US",
            "ts_ms": 1700000000000
        }
        
        response = client.post("/api/v1/score", json=tx_data)
        assert response.status_code == 200
        data = response.json()
        
        assert data["action"] == "DECLINE"
        assert "Amount exceeds maximum" in str(data.get("rules_triggered", []))
