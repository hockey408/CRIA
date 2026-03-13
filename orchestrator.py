from dotenv import load_dotenv
load_dotenv()

import json
from datetime import datetime
from langgraph.graph import StateGraph, END
from typing import TypedDict
from tools import load_portfolio, flag_anomalies, get_account, save_output
from memory import save_review, get_review_history, get_memory_stats, search_reviews
from reg_agent import run_reg_prep_agent, build_portfolio_context
import anthropic

client = anthropic.Anthropic()


class OrchestratorState(TypedDict):
    portfolio_path: str
    threshold: float
    df: object
    flagged_accounts: list
    all_results: list
    reg_prep_results: list
    run_date: str
    status: str


def load_data(state: OrchestratorState) -> OrchestratorState:
    df = load_portfolio(state["portfolio_path"])
    print(f"[Orchestrator] Portfolio loaded: {len(df)} accounts")
    return {**state, "df": df, "status": "data_loaded"}


def run_anomaly_review(state: OrchestratorState) -> OrchestratorState:
    """Runs the full anomaly review loop."""
    print("\n[Orchestrator] Starting anomaly review agent...")
    df = state["df"]
    flagged = flag_anomalies(df, state["threshold"])
    account_ids = flagged["account_id"].tolist()

    results = []
    for account_id in account_ids:
        try:
            account = get_account(df, account_id)
            history = get_review_history(account_id)

            history_context = ""
            if history:
                history_context = "\n\nPrior review history:\n"
                for h in history:
                    history_context += f"- {h['review_date']}: {h['narrative']}\n"

            prompt = f"""You are a credit risk analyst at a bank's Second Line of Defense.

Account flagged by anomaly detection:
{json.dumps(account, indent=2)}
{history_context}

Write a concise anomaly review narrative (3-4 sentences). If prior history exists,
explicitly reference it and note whether the situation has improved, deteriorated,
or remained stable. Be direct and professional."""

            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}]
            )
            narrative = response.content[0].text
            save_review(account, narrative)

            results.append({
                **account,
                "narrative": narrative,
                "had_prior_history": len(history) > 0
            })
            print(f"  Reviewed {account_id} ✓")

        except Exception as e:
            print(f"  Skipped {account_id} — {e}")
            continue

    print(f"[Orchestrator] Anomaly review complete: {len(results)} accounts reviewed")
    return {**state, "all_results": results, "status": "anomaly_review_complete"}


def run_regulatory_prep(state: OrchestratorState) -> OrchestratorState:
    """Runs the regulatory prep agent using anomaly review outputs as context."""
    print("\n[Orchestrator] Starting regulatory prep agent...")
    results = run_reg_prep_agent()
    print(f"[Orchestrator] Regulatory prep complete: {len(results)} responses drafted")
    return {**state, "reg_prep_results": results, "status": "reg_prep_complete"}


def compile_final_report(state: OrchestratorState) -> OrchestratorState:
    """Compiles both agents' outputs into a single report package."""
    print("\n[Orchestrator] Compiling final report...")

    stats = get_memory_stats()
    flagged = flag_anomalies(state["df"], state["threshold"])
    portfolio_context = build_portfolio_context(state["df"], flagged)

    report = {
        "run_metadata": {
            "run_date": state["run_date"],
            "portfolio_path": state["portfolio_path"],
            "threshold": state["threshold"],
            "total_reviews": stats["total_reviews"]
        },
        "portfolio_summary": portfolio_context,
        "anomaly_reviews": state["all_results"],
        "regulatory_prep": state["reg_prep_results"]
    }

    with open("cria_full_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("[Orchestrator] Full report saved to cria_full_report.json")
    return {**state, "status": "complete"}


def build_orchestrator():
    graph = StateGraph(OrchestratorState)
    graph.add_node("load_data", load_data)
    graph.add_node("run_anomaly_review", run_anomaly_review)
    graph.add_node("run_regulatory_prep", run_regulatory_prep)
    graph.add_node("compile_final_report", compile_final_report)
    graph.set_entry_point("load_data")
    graph.add_edge("load_data", "run_anomaly_review")
    graph.add_edge("run_anomaly_review", "run_regulatory_prep")
    graph.add_edge("run_regulatory_prep", "compile_final_report")
    graph.add_edge("compile_final_report", END)
    return graph.compile()


if __name__ == "__main__":
    print("=" * 50)
    print("CRIA — Credit Risk Intelligence Agent")
    print("Multi-Agent Orchestrator")
    print("=" * 50)

    orchestrator = build_orchestrator()
    initial_state = {
        "portfolio_path": "portfolio_enriched.csv",
        "threshold": 2.5,
        "df": None,
        "flagged_accounts": [],
        "all_results": [],
        "reg_prep_results": [],
        "run_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "status": "initialized"
    }

    final_state = orchestrator.invoke(initial_state)
    print("\n" + "=" * 50)
    print(f"CRIA run complete.")
    print(f"  Anomaly reviews: {len(final_state['all_results'])}")
    print(f"  Examiner responses: {len(final_state['reg_prep_results'])}")
    print(f"  Full report: cria_full_report.json")
    print("=" * 50)