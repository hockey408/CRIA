from tools import load_portfolio, process_flagged_accounts, save_output

df = load_portfolio("portfolio_enriched.csv")
results = process_flagged_accounts(df, threshold=2.5, max_accounts=5)

save_output(results, "batch_output.json")
print(f"\nCompleted: {len(results)} narratives generated")