"""E2E Load Testing Script (Phase 5).

Fires transactions at the Scoring API as fast as possible to measure
TPS, Latency (p50, p95, p99), and verify the rules engine handles load.

Usage::

    # Terminal 1: Start API
    python -m uvicorn scripts.api.main:app --host 0.0.0.0 --port 8000 --workers 4

    # Terminal 2: Run Load Test
    python -m scripts.tests.load_test --target http://localhost:8000/api/v1/score --requests 10000 --concurrency 50
"""
from __future__ import annotations

import argparse
import asyncio
import time
from collections import Counter

import httpx
import numpy as np

# Sample transaction base
BASE_TX = {
    "tx_id": "tx_bench",
    "user_id": "test_load_user",
    "amount_minor": 1500,
    "currency": "USD",
    "merchant_id": "merch_123",
    "mcc": "5411",
    "channel": "ecommerce",
    "country": "US",
    "ts_ms": int(time.time() * 1000)
}


async def _worker(client: httpx.AsyncClient, target: str, requests_to_make: int, latencies: list, actions: Counter):
    for i in range(requests_to_make):
        tx = BASE_TX.copy()
        tx["tx_id"] = f"tx_bench_{time.time_ns()}"
        
        # Inject some variance to hit different rules
        if i % 100 == 0:
            tx["amount_minor"] = 2000000  # Will trigger DECLINE
        
        start = time.perf_counter()
        try:
            resp = await client.post(target, json=tx)
            lat = (time.perf_counter() - start) * 1000
            latencies.append(lat)
            
            if resp.status_code == 200:
                data = resp.json()
                actions[data["action"]] += 1
            else:
                actions["ERROR"] += 1
        except Exception:
            actions["ERROR"] += 1


async def run_load_test(target: str, total_requests: int, concurrency: int):
    print(f"Starting Load Test against {target}")
    print(f"Total Requests: {total_requests} | Concurrency: {concurrency}")
    print("-" * 50)
    
    latencies = []
    actions = Counter()
    reqs_per_worker = total_requests // concurrency
    
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    
    start_time = time.perf_counter()
    
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        tasks = [
            _worker(client, target, reqs_per_worker, latencies, actions)
            for _ in range(concurrency)
        ]
        await asyncio.gather(*tasks)
        
    duration = time.perf_counter() - start_time
    
    # Print Results
    print(f"\nRESULTS:")
    print(f"Duration: {duration:.2f} seconds")
    print(f"TPS:      {len(latencies) / duration:.2f} req/s")
    print("-" * 50)
    
    if latencies:
        print(f"Latency P50: {np.percentile(latencies, 50):.2f} ms")
        print(f"Latency P90: {np.percentile(latencies, 90):.2f} ms")
        print(f"Latency P95: {np.percentile(latencies, 95):.2f} ms")
        print(f"Latency P99: {np.percentile(latencies, 99):.2f} ms")
        print(f"Latency Max: {np.max(latencies):.2f} ms")
    else:
        print("No successful requests completed.")
        
    print("-" * 50)
    for k, v in actions.items():
        print(f"Action [{k}]: {v}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=str, default="http://localhost:8000/api/v1/score")
    parser.add_argument("--requests", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=100)
    args = parser.parse_args()
    
    asyncio.run(run_load_test(args.target, args.requests, args.concurrency))


if __name__ == "__main__":
    main()
