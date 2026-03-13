from tools import load_portfolio, flag_anomalies, get_account, draft_narrative, save_output

# Test each function independently
df = load_portfolio("portfolio_enriched.csv")
print(df.shape)

flagged = flag_anomalies(df, threshold=2.5)
print(flagged[["account_id", "z_score", "severity"]].head())

# Grab the first flagged account ID dynamically
first_flagged_id = flagged.iloc[0]["account_id"]
account = get_account(df, first_flagged_id)
print(account)

narrative = draft_narrative(account)
print(narrative)

save_output({**account, "narrative": narrative}, "test_output.json")