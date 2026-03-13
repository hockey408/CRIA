import pandas as pd
import random

random.seed(42)

segments = ["C&I", "CRE", "Consumer", "SBA"]
ratings = ["Pass", "Watch", "Substandard", "Doubtful"]

records = []
for i in range(1, 101):
    z = round(random.gauss(1.2, 0.9), 2)
    records.append({
        "account_id": f"ACC{i:03d}",
        "segment": random.choice(segments),
        "outstanding_balance": round(random.uniform(100_000, 10_000_000), 2),
        "z_score": z,
        "composite_severity": round(abs(z) * random.uniform(0.8, 1.2), 2),
        "rating": random.choice(ratings),
        "months_on_book": random.randint(6, 120)
    })

df = pd.DataFrame(records)
df.to_csv("portfolio.csv", index=False)
print("Created portfolio.csv with 100 accounts")