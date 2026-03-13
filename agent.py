from langgraph.graph import StateGraph, END
from typing import TypedDict
from tools import load_portfolio, flag_anomalies, get_account, draft_narrative, save_output
from memory import save_review, get_review_history, get_memory_stats
import json
import anthropic

client = anthropic.Anthropic()


class AgentState(TypedDict):
    portfolio_path: str
    threshold: float
    df: object
    flagged_accounts: list
    current_account: dict
    narrative: str
    all_results: list
    status: str


def load_data(state: AgentState) -> AgentState:
    df = load_portfolio(state["portfolio_path"])
    return {**state, "df": df, "status": "data_loaded"}


def identify_anomalies(state: AgentState) -> AgentState:
    flagged = flag_anomalies(state["df"], state["threshold"])
    account_ids = flagged["account_id"].tolist()
    print(f"Identified {len(account_ids)} accounts to review")
    return {**state, "flagged_accounts": account_ids, "status": "anomalies_identified"}


def review_next_account(state: AgentState) -> AgentState:
    if not state["flagged_accounts"]:
        return {**state, "status": "complete"}

    remaining = state["flagged_accounts"].copy()
    account_id = remaining.pop(0)
    account = get_account(state["df"], account_id)

    # Check memory for prior reviews of this account
    history = get_review_history(account_id)
    
    if history:
        print(f"  Found {len(history)} prior review(s) for {account_id} — adding context")
        history_context = f"\n\nPrior review history for this account:\n"
        for h in history:
            history_context += f"- {h['review_date']}: {h['narrative']}\n"
    else:
        history_context = "\n\nNo prior review history for this account."

    # Build memory-aware prompt
    prompt = f"""You are a credit risk analyst at a bank's Second Line of Defense.

The following account has been flagged by an anomaly detection model:

{json.dumps(account, indent=2)}
{history_context}

Write a concise anomaly review narrative (3-4 sentences) that:
1. States what was flagged and why it's notable
2. References prior history if available and notes any trend
3. Recommends a next step for the credit officer

Be direct and professional."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    narrative = response.content[0].text

    # Save to persistent memory
    save_review(account, narrative)

    result = {**account, "narrative": narrative, "had_prior_history": len(history) > 0}
    all_results = state.get("all_results", []) + [result]

    print(f"Reviewed {account_id} ✓")
    return {**state,
            "flagged_accounts": remaining,
            "current_account": account,
            "narrative": narrative,
            "all_results": all_results,
            "status": "account_reviewed"}


def save_results(state: AgentState) -> AgentState:
    save_output(state["all_results"], "agent_output.json")
    stats = get_memory_stats()
    print(f"\nMemory now contains {stats['total_reviews']} total reviews")
    return {**state, "status": "saved"}


def should_continue(state: AgentState) -> str:
    if state["flagged_accounts"]:
        return "review_next_account"
    else:
        return "save_results"


def build_agent():
    graph = StateGraph(AgentState)
    graph.add_node("load_data", load_data)
    graph.add_node("identify_anomalies", identify_anomalies)
    graph.add_node("review_next_account", review_next_account)
    graph.add_node("save_results", save_results)
    graph.set_entry_point("load_data")
    graph.add_edge("load_data", "identify_anomalies")
    graph.add_edge("identify_anomalies", "review_next_account")
    graph.add_conditional_edges("review_next_account", should_continue)
    graph.add_edge("save_results", END)
    return graph.compile()


if __name__ == "__main__":
    agent = build_agent()
    initial_state = {
        "portfolio_path": "portfolio_enriched.csv",
        "threshold": 2.5,
        "df": None,
        "flagged_accounts": [],
        "current_account": {},
        "narrative": "",
        "all_results": [],
        "status": "initialized"
    }
    final_state = agent.invoke(initial_state)
    print(f"\nAgent complete. Reviewed {len(final_state['all_results'])} accounts.")