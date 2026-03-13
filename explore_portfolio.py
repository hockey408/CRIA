import pandas as pd

df = pd.read_csv("portfolio.csv")

# Basic exploration — mirrors what you'd do in SQL first
print(df.shape)          # rows, columns
print(df.dtypes)         # column types
print(df.head(10))       # first 10 rows
print(df.describe())     # summary stats

# Filtering — equivalent to WHERE clause
anomalies = df[df["z_score"] >= 2.5]
print(f"\nFlagged accounts: {len(anomalies)}")

# Filtering with multiple conditions — AND/OR
high_severity = df[(df["z_score"] >= 2.5) & (df["segment"] == "CRE")]

# Selecting columns — equivalent to SELECT
summary = df[["account_id", "segment", "z_score", "rating"]]

# Aggregation — equivalent to GROUP BY
by_segment = df.groupby("segment").agg(
    account_count=("account_id", "count"),
    avg_z_score=("z_score", "mean"),
    total_balance=("outstanding_balance", "sum")
).round(2)

print(by_segment)

# Adding a derived column — equivalent to CASE WHEN
def severity_label(z):
    if z >= 3.0: return "HIGH"
    elif z >= 2.5: return "MEDIUM"
    else: return "NORMAL"

df["severity"] = df["z_score"].apply(severity_label)

# Save the enriched version
df.to_csv("portfolio_enriched.csv", index=False)