from dotenv import load_dotenv
load_dotenv()

import anthropic
import json
import pandas as pd
from tools import load_portfolio, flag_anomalies
from memory import search_reviews, get_memory_stats

client = anthropic.Anthropic()

# Common examiner question templates relevant to credit risk
EXAMINER_QUESTIONS = [
    "What is the current state of your anomaly detection methodology and how are exceptions escalated?",
    "Describe the concentration risk in your flagged portfolio by segment.",
    "What trends are you observing in your watch-rated credits and what is the disposition plan?",
    "How does your Second Line of Defense validate the accuracy of First Line credit risk ratings?"
]


def build_portfolio_context(df: pd.DataFrame, flagged: pd.DataFrame) -> dict:
    """
    Builds a structured summary of portfolio data for regulatory context.
    """
    segment_summary = flagged.groupby("segment").agg(
        count=("account_id", "count"),
        avg_z_score=("z_score", "mean"),
        total_balance=("outstanding_balance", "sum")
    ).round(2).to_dict(orient="index")

    rating_summary = flagged.groupby("rating").size().to_dict()

    high_severity = flagged[flagged["z_score"] >= 3.0]

    return {
        "total_portfolio_accounts": int(len(df)),
        "total_flagged_accounts": int(len(flagged)),
        "flag_rate_pct": round(len(flagged) / len(df) * 100, 1),
        "segment_breakdown": segment_summary,
        "rating_breakdown": rating_summary,
        "high_severity_count": int(len(high_severity)),
        "avg_z_score_flagged": round(float(flagged["z_score"].mean()), 2)
    }


def draft_examiner_response(question: str, portfolio_context: dict, memory_context: str) -> str:
    """
    Drafts a regulatory examiner response using portfolio data and review history.
    """
    prompt = f"""You are a Senior Credit Risk Officer preparing written responses for a federal bank examination.

PORTFOLIO DATA:
{json.dumps(portfolio_context, indent=2)}

REVIEW HISTORY CONTEXT:
{memory_context}

EXAMINER QUESTION:
{question}

Draft a professional, defensible response (4-6 sentences) that:
1. Directly answers the examiner's question
2. Cites specific data points from the portfolio where relevant
3. Demonstrates methodology rigor and governance awareness
4. Uses language appropriate for a formal regulatory examination response

Be precise and confident. Avoid vague language. Regulators respond well to specificity."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text


def run_reg_prep_agent(questions: list = None) -> list:
    """
    Main entry point for the regulatory prep agent.
    Processes a list of examiner questions and returns drafted responses.
    """
    if questions is None:
        questions = EXAMINER_QUESTIONS

    # Load portfolio data
    df = load_portfolio("portfolio_enriched.csv")
    flagged = flag_anomalies(df, threshold=2.5)
    portfolio_context = build_portfolio_context(df, flagged)

    # Pull relevant memory context
    memory_results = search_reviews("anomaly review findings credit risk", n_results=5)
    if memory_results:
        memory_context = "\n".join([
            f"- {r['account_id']} ({r['segment']}, {r['review_date']}): {r['narrative']}"
            for r in memory_results
        ])
    else:
        memory_context = "No prior review history available."

    # Draft responses
    results = []
    for i, question in enumerate(questions, 1):
        print(f"Drafting response {i}/{len(questions)}...")
        response = draft_examiner_response(question, portfolio_context, memory_context)
        results.append({
            "question": question,
            "drafted_response": response
        })

    return results


if __name__ == "__main__":
    print("Running Regulatory Prep Agent...\n")
    results = run_reg_prep_agent()

    # Save output
    with open("reg_prep_output.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nCompleted. {len(results)} examiner responses drafted.")
    print("Output saved to reg_prep_output.json")