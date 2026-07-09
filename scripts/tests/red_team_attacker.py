"""Red Team Attack Simulator for Aegis FDS.

Executes predefined attack vectors to stress test the API and Rules Engine:
- DDoS/Cache Stampede (Hits API directly)
- Card Testing (Velocity - Hits Kafka, then API)
- Smurfing (Structuring - Hits Kafka, then API)
- Account Takeover (ATO - Hits API directly)

Usage::
    python -m scripts.tests.red_team_attacker --mode all
"""
from __future__ import annotations

import argparse
import asyncio
import time
from collections import Counter
from typing import Any

import httpx
from quixstreams import Application

TARGET_URL = "http://localhost:8001/api/v1/score"
KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "fds.tx.raw.v1"

def _make_tx(user_id: str, amount: int, mcc: str = "5411", country: str = "US") -> dict[str, Any]:
    return {
        "tx_id": f"tx_rt_{time.time_ns()}",
        "user_id": user_id,
        "amount_minor": amount,
        "currency": "USD",
        "merchant_id": "merch_rt",
        "mcc": mcc,
        "channel": "ecommerce",
        "country": country,
        "ts_ms": int(time.time() * 1000)
    }

async def attack_ddos(client: httpx.AsyncClient) -> list[dict]:
    print("[*] Launching DDoS / Cache Stampede Attack (1000 concurrent reqs to 1 user)...")
    user_id = "victim_ddos_001"
    reqs = [_make_tx(user_id, 100) for _ in range(1000)]
    
    start_time = time.perf_counter()
    tasks = [client.post(TARGET_URL, json=tx) for tx in reqs]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    duration = time.perf_counter() - start_time
    
    results = []
    for r in responses:
        if isinstance(r, Exception):
            results.append({"status": "ERROR", "action": "ERROR"})
        elif r.status_code == 200:
            results.append({"status": 200, "action": r.json().get("action")})
        else:
            results.append({"status": r.status_code, "action": "ERROR"})
            
    print(f"    -> DDoS completed in {duration:.2f}s")
    return results

async def attack_card_testing(client: httpx.AsyncClient, producer, topic) -> list[dict]:
    print("[*] Launching Card Testing Attack (Velocity Fraud via Kafka)...")
    user_id = "victim_cardtest_002"
    results = []
    
    # 1. 10 small transactions via Kafka to build state
    for _ in range(10):
        tx = _make_tx(user_id, 100) # $1
        msg = topic.serialize(key=tx["user_id"], value=tx)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value)
        results.append({"action": "KAFKA_INGEST"})
        
    producer.flush()
    print("    -> Waiting 3s for Stream Processor to crunch Sliding Windows...")
    await asyncio.sleep(3)
        
    # 2. 1 large cashout via API
    tx_large = _make_tx(user_id, 500000) # $5000
    r = await client.post(TARGET_URL, json=tx_large)
    results.append(r.json() if r.status_code == 200 else {"action": "ERROR"})
    
    print("    -> Card Testing phase complete")
    return results

async def attack_smurfing(client: httpx.AsyncClient, producer, topic) -> list[dict]:
    print("[*] Launching Smurfing / Structuring Attack (via Kafka)...")
    user_id = "victim_smurf_003"
    results = []
    
    # 4 transactions of $9500 to Kafka
    for _ in range(4):
        tx = _make_tx(user_id, 950000)
        msg = topic.serialize(key=tx["user_id"], value=tx)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value)
        results.append({"action": "KAFKA_INGEST"})
        
    producer.flush()
    print("    -> Waiting 3s for Stream Processor to aggregate amt_sum_1h...")
    await asyncio.sleep(3)
    
    # 5th transaction via API
    tx = _make_tx(user_id, 950000)
    r = await client.post(TARGET_URL, json=tx)
    results.append(r.json() if r.status_code == 200 else {"action": "ERROR"})
        
    print("    -> Smurfing phase complete")
    return results

async def attack_ato(client: httpx.AsyncClient) -> list[dict]:
    print("[*] Launching ATO Attack (Location Spoofing)...")
    user_id = "victim_ato_004"
    tx = _make_tx(user_id, 600000, country="RU")
    
    r = await client.post(TARGET_URL, json=tx)
    result = [r.json() if r.status_code == 200 else {"action": "ERROR"}]
    print("    -> ATO phase complete")
    return result

def print_report(name: str, results: list[dict]):
    actions = Counter([r.get("action") for r in results if r.get("action") != "KAFKA_INGEST"])
    print(f"\n{'='*40}")
    print(f"REPORT: {name}")
    print(f"{'='*40}")
    
    if name == "DDoS":
        print(f"Total Requests: {len(results)}")
        for action, count in actions.items():
            print(f"  - {action}: {count}")
        success_rate = (actions.get("APPROVE", 0) + actions.get("DECLINE", 0)) / len(results)
        print(f"System Resilience (Non-error): {success_rate*100:.1f}%")
        
    elif name == "Card Testing":
        last_action = results[-1].get("action")
        print(f"10 Auth requests sent to Kafka")
        print(f"Cashout Attempt Action (API): {last_action}")
        if results[-1].get("rules_triggered"):
             print(f"Rules triggered: {results[-1].get('rules_triggered')}")
             
    elif name == "Smurfing":
        last_action = results[-1].get("action")
        print(f"4 transactions ($38k) sent to Kafka")
        print(f"5th Transaction Attempt Action (API): {last_action}")
        if results[-1].get("rules_triggered"):
             print(f"Rules triggered: {results[-1].get('rules_triggered')}")
             
    elif name == "ATO":
        print(f"ATO Transaction Action (API): {results[0].get('action')}")
        if results[0].get("rules_triggered"):
            print(f"Rules triggered: {results[0].get('rules_triggered')}")

async def main(mode: str):
    print("\n" + "#"*50)
    print("AEGIS FDS - RED TEAM ATTACK SIMULATOR")
    print("#"*50 + "\n")
    
    # Initialize Kafka App & Topic
    app = Application(broker_address=KAFKA_BROKER)
    topic = app.topic(TOPIC_NAME, value_serializer="json")
    
    limits = httpx.Limits(max_connections=1000)
    
    with app.get_producer() as producer:
        async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
            
            if mode in ("ddos", "all"):
                res = await attack_ddos(client)
                print_report("DDoS", res)
                
            if mode in ("card_testing", "all"):
                res = await attack_card_testing(client, producer, topic)
                print_report("Card Testing", res)
                
            if mode in ("smurfing", "all"):
                res = await attack_smurfing(client, producer, topic)
                print_report("Smurfing", res)
                
            if mode in ("ato", "all"):
                res = await attack_ato(client)
                print_report("ATO", res)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["ddos", "card_testing", "smurfing", "ato", "all"], default="all")
    args = parser.parse_args()
    
    asyncio.run(main(args.mode))
