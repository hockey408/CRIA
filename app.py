import streamlit as st
import anthropic
import pandas as pd
import json
from tools import load_portfolio, flag_anomalies, get_account
from memory import search_reviews, get_review_history, get_memory_stats, save_review

# Page config
st.set_page_config(
    page_title="CRIA — Credit Risk Intelligence Agent",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Credit Risk Intelligence Agent")
st.caption("Ask questions about your portfolio in plain English")

# Load data once and cache it
@st.cache_data
def load_data():
    df = load_portfolio("portfolio_enriched.csv")
    return df

df = load_data()
flagged = flag_anomalies(df, threshold=2.5)

# Sidebar — portfolio stats
with st.sidebar:
    st.header("Portfolio Summary")
    st.metric("Total Accounts", len(df))
    st.metric("Flagged Accounts", len(flagged))
    st.metric("Flag Rate", f"{len(flagged)/len(df)*100:.1f}%")
    
    stats = get_memory_stats()
    st.metric("Reviews in Memory", stats["total_reviews"])
    
    st.divider()
    st.subheader("Flagged Accounts")
    st.dataframe(
        flagged[["account_id", "segment", "z_score", "severity"]].reset_index(drop=True),
        use_container_width=True
    )

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.messages.append({
        "role": "assistant",
        "content": "Hello. I'm your Credit Risk Intelligence Agent. I have access to your portfolio data and review history. You can ask me things like:\n\n- *Summarize the flagged accounts*\n- *What CRE accounts are flagged?*\n- *Show me the history for ACC001*\n- *Which segment has the most anomalies?*"
    })

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about your portfolio..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Build context for Claude
    flagged_summary = flagged[["account_id", "segment", "z_score", "severity", "rating"]].to_dict(orient="records")
    segment_counts = flagged.groupby("segment").size().to_dict()
    
    # Search memory for relevant prior reviews
    memory_results = search_reviews(prompt, n_results=3)
    memory_context = ""
    if memory_results:
        memory_context = "\n\nRelevant prior reviews from memory:\n"
        for r in memory_results:
            memory_context += f"- {r['account_id']} ({r['segment']}, {r['review_date']}): {r['narrative'][:150]}...\n"

    system_prompt = f"""You are CRIA, a Credit Risk Intelligence Agent for a bank's Second Line of Defense team.

You have access to the following portfolio data:

FLAGGED ACCOUNTS ({len(flagged_summary)} total):
{json.dumps(flagged_summary, indent=2)}

ANOMALIES BY SEGMENT:
{json.dumps(segment_counts, indent=2)}

TOTAL PORTFOLIO: {len(df)} accounts
{memory_context}

Answer the analyst's question concisely and professionally. 
If asked about a specific account, provide detail. 
If asked for a summary, be structured and clear.
Use credit risk terminology appropriate for a Second Line of Defense audience."""

    # Build conversation history for multi-turn context
    api_messages = []
    for msg in st.session_state.messages[1:]:  # skip the initial assistant greeting
        api_messages.append({"role": msg["role"], "content": msg["content"]})

    client = anthropic.Anthropic()
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=system_prompt,
        messages=api_messages
    )

    reply = response.content[0].text
    st.session_state.messages.append({"role": "assistant", "content": reply})
    with st.chat_message("assistant"):
        st.markdown(reply)