import anthropic
import pandas as pd
import json

client = anthropic.Anthropic()

df = pd.read_csv("portfolio_enriched.csv")

# Pull one flagged account
flagged = df[df["severity"].isin(["HIGH", "MEDIUM"])].head(1).iloc[0]

# Build a structured prompt
account_data = {
    "account_id": flagged["account_id"],
    "segment": flagged["segment"],
    "outstanding_balance": f"${flagged['outstanding_balance']:,.0f}",
    "z_score": flagged["z_score"],
    "composite_severity": flagged["composite_severity"],
    "rating": flagged["rating"],
    "months_on_book": flagged["months_on_book"]
}

prompt = f"""You are a credit risk analyst at a bank's Second Line of Defense.

The following account has been flagged by an anomaly detection model:

{json.dumps(account_data, indent=2)}

Write a concise anomaly review narrative (3-4 sentences) that:
1. States what was flagged and why it's notable
2. Notes any contextual factors from the data
3. Recommends a next step for the credit officer

Be direct and professional. Use language appropriate for internal credit risk reporting."""

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=300,
    messages=[{"role": "user", "content": prompt}]
)

narrative = response.content[0].text
print(f"\nAccount: {account_data['account_id']}")
print(f"Segment: {account_data['segment']}")
print(f"Z-Score: {account_data['z_score']}")
print(f"\nGenerated Narrative:\n{narrative}")

# Save to file
output = {**account_data, "narrative": narrative}
with open("anomaly_review_output.json", "w") as f:
    json.dump(output, f, indent=2)