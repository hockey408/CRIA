from memory import save_review, search_reviews, get_review_history, get_memory_stats

# Save a couple of test reviews
test_account_1 = {
    "account_id": "ACC001",
    "segment": "CRE",
    "z_score": 3.15,
    "severity": "HIGH",
    "rating": "Watch",
    "outstanding_balance": "$4,250,000"
}

test_account_2 = {
    "account_id": "ACC002",
    "segment": "C&I",
    "z_score": 2.74,
    "severity": "MEDIUM",
    "rating": "Pass",
    "outstanding_balance": "$1,800,000"
}

save_review(test_account_1, "CRE account flagged with high z-score. Recommend immediate credit officer review.")
save_review(test_account_2, "C&I account showing moderate anomaly. Monitor for trend continuation.")

# Test semantic search
print("\n--- Search: 'high risk real estate' ---")
results = search_reviews("high risk real estate", n_results=2)
for r in results:
    print(f"  {r['account_id']} | {r['segment']} | z={r['z_score']}")

# Test account history
print("\n--- History for ACC001 ---")
history = get_review_history("ACC001")
for h in history:
    print(f"  Reviewed: {h['review_date']} | {h['narrative'][:80]}...")

# Stats
print("\n--- Memory Stats ---")
print(get_memory_stats())