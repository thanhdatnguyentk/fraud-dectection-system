import pytest
from datetime import datetime, timedelta

# Dựa theo 5.4 Scenarios tự động trong data-synthesis-plan.md
# Data generator cho streaming test

def test_velocity_attack_scenario():
    """
    Scenario: velocity_attack (30 tx trong 60 s cùng card)
    Mục đích: Hard rule + Redis counter
    """
    transactions = []
    base_time = datetime.now()
    card_bin = "448588"
    
    for i in range(30):
        transactions.append({
            "tx_id": f"tx_vel_{i}",
            "card_bin": card_bin,
            "ts_ms": int((base_time + timedelta(seconds=i*2)).timestamp() * 1000),
            "amount_minor": 15000, # 150.00 USD
            "label": 1 # inject nhãn giả
        })
    
    assert len(transactions) == 30
    assert all(tx["card_bin"] == card_bin for tx in transactions)
    
    time_diff_ms = transactions[-1]["ts_ms"] - transactions[0]["ts_ms"]
    assert time_diff_ms <= 60000, "Tất cả giao dịch phải nằm trong khoảng 60s"

def test_impossible_travel_scenario():
    """
    Scenario: impossible_travel (NY → Tokyo trong 5 phút)
    Mục đích: Flink window + geo-distance
    """
    user_id = "user_travel_01"
    
    tx_ny = {
        "tx_id": "tx_ny",
        "user_id": user_id,
        "lat": 40.7128,
        "lon": -74.0060,
        "ts_ms": 1600000000000,
        "country": "US"
    }
    
    tx_tokyo = {
        "tx_id": "tx_tokyo",
        "user_id": user_id,
        "lat": 35.6762,
        "lon": 139.6503,
        "ts_ms": 1600000000000 + 300000, # 5 phút sau
        "country": "JP"
    }
    
    time_diff_ms = tx_tokyo["ts_ms"] - tx_ny["ts_ms"]
    assert time_diff_ms == 300000, "Khoảng cách thời gian phải là 5 phút (300000 ms)"
    assert tx_ny["country"] != tx_tokyo["country"]

def test_device_spray_scenario():
    """
    Scenario: device_spray (1 device, 50 user khác nhau)
    Mục đích: GNN/clustering
    """
    device_fp = "fp_hacker_device_001"
    transactions = []
    
    for i in range(50):
        transactions.append({
            "tx_id": f"tx_spray_{i}",
            "device_fp": device_fp,
            "user_id": f"user_victim_{i}"
        })
        
    unique_users = set(tx["user_id"] for tx in transactions)
    assert len(unique_users) == 50
    assert all(tx["device_fp"] == device_fp for tx in transactions)

def test_fat_finger_scenario():
    """
    Scenario: fat_finger (amount gấp 10x lần trước của user)
    Mục đích: XGBoost feature
    """
    tx_normal = {
        "tx_id": "tx_norm",
        "user_id": "user_fat",
        "amount_minor": 5000 # 50 USD
    }
    
    tx_fat = {
        "tx_id": "tx_fat",
        "user_id": "user_fat",
        "amount_minor": tx_normal["amount_minor"] * 12 # Gấp 12 lần
    }
    
    assert tx_fat["amount_minor"] >= tx_normal["amount_minor"] * 10
