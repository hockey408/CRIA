from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import json
import anthropic
import os

client = anthropic.Anthropic()

def load_portfolio(filepath: str) -> pd.DataFrame:
    """
    Loads a credit portfolio CSV and returns a DataFrame.
    Raises a clear error if the file doesn't exist.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Portfolio file not found: {filepath}")
    
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} accounts from {filepath}")
    return df


def flag_anomalies(df: pd.DataFrame, threshold: float = 2.5) -> pd.DataFrame:
    """
    Filters a portfolio DataFrame to accounts exceeding the z-score threshold.
    Returns a new DataFrame of flagged accounts only.
    """
    flagged = df[df["z_score"] >= threshold].copy()
    flagged["severity"] = flagged["z_score"].apply(
        lambda z: "HIGH" if z >= 3.0 else "MEDIUM"
    )
    print(f"Flagged {len(flagged)} accounts at threshold {threshold}")
    return flagged


def get_account(df: pd.DataFrame, account_id: str) -> dict:
    """
    Pulls a single account by ID and returns it as a clean Python dict.
    """
    match = df[df["account_id"] == account_id]
    
    if match.empty:
        raise ValueError(f"Account {account_id} not found in portfolio")
    
    row = match.iloc[0]
    return {
        "account_id": str(row["account_id"]),
        "segment": str(row["segment"]),
        "outstanding_balance": f"${row['outstanding_balance']:,.0f}",
        "z_score": float(row["z_score"]),
        "composite_severity": float(row["composite_severity"]),
        "rating": str(row["rating"]),
        "months_on_book": int(row["months_on_book"])
    }


def draft_narrative(account_data: dict) -> str:
    """
    Calls Claude to generate an anomaly review narrative for a single account.
    Returns the narrative as a string.
    """
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
    return response.content[0].text


def save_output(data: dict, filepath: str) -> None:
    """
    Saves a dictionary as a formatted JSON file.
    """
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved output to {filepath}")

def process_flagged_accounts(df: pd.DataFrame, threshold: float = 2.5, max_accounts: int = 5) -> list:
    """
    Processes all flagged accounts up to max_accounts.
    Skips individual failures without stopping the whole run.
    Returns a list of output dicts.
    """
    flagged = flag_anomalies(df, threshold)
    results = []
    
    for _, row in flagged.head(max_accounts).iterrows():
        account_id = row["account_id"]
        try:
            account = get_account(df, account_id)
            narrative = draft_narrative(account)
            result = {**account, "narrative": narrative}
            results.append(result)
            print(f"Processed {account_id} ✓")
        except Exception as e:
            print(f"Skipped {account_id} — error: {e}")
            continue
    
    return results