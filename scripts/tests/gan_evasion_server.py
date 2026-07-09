import asyncio
import time
import random
import uuid
import httpx
from collections import Counter

# Target API
TARGET_URL = "http://localhost:8001/api/v1/score"

# We want to achieve a >10% Success Rate (Evasion Rate) for Fraudulent Cashouts.
# The Discriminator is the FastAPI system (XGBoost + Rules Engine).
# The Generator is this adversarial logic trying to inject noise and bypass limits.

class TabularEvasionGenerator:
    """
    Acts as the 'Generator' in an adversarial setup.
    Instead of a traditional differentiable GAN (since XGBoost is non-differentiable 
    and acts as a Black-Box Discriminator), we use an adaptive evasion strategy 
    (Zeroth-Order / Evolutionary Evasion).
    """
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.history = []
        
        # Generator Parameters (Latent Space)
        self.current_amt = 50000  # Start with $500
        self.mcc_list = ["5411", "5812", "5912", "4511"]
        self.country = "US"
        
        # Evasion constraints learned from failures
        self.velocity_cooldown = 1.0  # seconds to wait
        self.max_tx_burst = 4         # Keep below 5 to avoid velocity rules

    def generate_adversarial_tx(self) -> dict:
        """Generate a transaction with adversarial noise to fool the XGBoost model."""
        # 1. Add noise to amount to avoid exact pattern matching
        noise = random.randint(-5000, 5000)
        attack_amt = max(100, self.current_amt + noise)
        
        # 2. Pick a random MCC to keep distinct_mcc_30d feature high (which might lower fraud score in some models)
        mcc = random.choice(self.mcc_list)
        
        tx = {
            "tx_id": f"adv_{uuid.uuid4().hex[:8]}",
            "user_id": self.user_id,
            "amount_minor": attack_amt,
            "currency": "USD",
            "merchant_id": f"merch_adv_{random.randint(1, 100)}",
            "mcc": mcc,
            "channel": "ecommerce",
            "country": self.country,
            "ts_ms": int(time.time() * 1000)
        }
        return tx
        
    def update_policy(self, result: dict):
        """Update generator weights based on Discriminator (API) feedback."""
        action = result.get("action", "ERROR")
        score = result.get("score", 1.0)
        rules = result.get("rules_triggered", [])
        
        # If model caught us (Score > 0.70 or Rule triggered)
        if action != "APPROVE":
            # Penalize: Model learned our pattern. We must adapt.
            if "Velocity spike (Card Testing)" in rules:
                self.velocity_cooldown += 2.0  # Wait longer next time
                
            if "Smurfing detected" in rules or "Amount exceeds maximum" in rules:
                self.current_amt = int(self.current_amt * 0.5)  # Halve the amount
                
            if score > 0.70:
                # XGBoost caught the anomaly. Introduce more noise to MCC or change channel
                self.country = "US" # Fallback to US to avoid International rules
        else:
            # Reward: We bypassed the model! 
            # Slowly increase amount to maximize stolen funds without triggering thresholds
            self.current_amt = int(self.current_amt * 1.1)

async def launch_adversarial_swarm(num_attackers: int, target_tx_per_attacker: int):
    print("="*60)
    print("🚀 INITIALIZING GAN-INSPIRED ADVERSARIAL EVASION SERVER 🚀")
    print("="*60)
    print(f"Deploying {num_attackers} adversarial agents (Generators)...")
    
    actions_counter = Counter()
    total_stolen = 0
    total_tx = 0
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        
        for tx_idx in range(target_tx_per_attacker):
            print(f"\n[Wave {tx_idx+1}/{target_tx_per_attacker}] Generating adversarial samples...")
            
            # Swarm attacks concurrently
            tasks = []
            generators = [TabularEvasionGenerator(f"adv_user_{i}") for i in range(num_attackers)]
            
            for gen in generators:
                tx = gen.generate_adversarial_tx()
                tasks.append(client.post(TARGET_URL, json=tx))
                
            # Execute batch
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process Feedback (Discriminator Loss -> Generator Update)
            for gen, resp in zip(generators, responses):
                if isinstance(resp, Exception) or resp.status_code != 200:
                    continue
                    
                total_tx += 1
                data = resp.json()
                action = data["action"]
                actions_counter[action] += 1
                
                if action == "APPROVE":
                    total_stolen += tx["amount_minor"]
                
                # Feedback loop
                gen.update_policy(data)
                
            # Adaptive Cooldown to evade Stream Processor Time Windows
            await asyncio.sleep(2.0)
            
    # Calculate Evasion Rate
    success_rate = (actions_counter["APPROVE"] / total_tx) * 100 if total_tx > 0 else 0
    
    print("\n" + "="*60)
    print("📊 ADVERSARIAL TRAINING RESULTS (DISCRIMINATOR VS GENERATOR)")
    print("="*60)
    print(f"Total Attack Transactions: {total_tx}")
    print(f"Total Fraud Value Stolen:  ${total_stolen/100:,.2f}")
    print(f"Model Approvals (Evasions): {actions_counter['APPROVE']}")
    print(f"Model Reviews/Declines:     {actions_counter['REVIEW'] + actions_counter['DECLINE']}")
    print("-" * 60)
    
    if success_rate > 10.0:
        print(f"🎯 TARGET ACHIEVED: Fraud Evasion Rate is {success_rate:.1f}% (> 10%)")
        print("💡 The XGBoost model and Rule Engine were bypassed by adaptive noise.")
    else:
        print(f"🛡️ FAILED TO BYPASS: Evasion Rate is {success_rate:.1f}%")
        print("💡 The Discriminator (API) is too robust for this generator configuration.")

if __name__ == "__main__":
    # Launch 50 attackers, each trying 5 sequential transactions to build history and bypass features
    asyncio.run(launch_adversarial_swarm(num_attackers=50, target_tx_per_attacker=5))
