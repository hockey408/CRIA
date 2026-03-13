# CRIA — Credit Risk Intelligence Agent

A locally-running multi-agent AI system for credit risk anomaly review and 
regulatory examination preparation, built as an independent learning project 
to develop applied AI engineering skills in a financial services context.

## What It Does

- **Anomaly Detection Pipeline** — Ingests a credit portfolio, applies 
  statistical flagging logic, and routes flagged accounts through an LLM-powered 
  review agent that drafts analyst-grade narratives
- **Persistent Memory** — Uses ChromaDB vector storage to retain review history 
  across runs, enabling the agent to detect trends and reference prior decisions
- **Regulatory Prep Agent** — A second specialized agent that translates 
  portfolio findings into drafted responses to federal examiner questions
- **Multi-Agent Orchestrator** — A LangGraph-based orchestrator that coordinates 
  both agents sequentially and compiles a unified report package
- **Conversational Interface** — A Streamlit chat UI for querying portfolio data 
  and review history in plain English

## Architecture
```
User Query / Scheduled Run
    → LangGraph Orchestrator
        → Anomaly Review Agent
            → Portfolio Loader (pandas)
            → Anomaly Flagging (z-score)
            → Memory Lookup (ChromaDB)
            → Narrative Drafter (Claude API)
            → Memory Writer (ChromaDB)
        → Regulatory Prep Agent
            → Portfolio Context Builder
            → Examiner Response Drafter (Claude API)
        → Report Compiler
    → cria_full_report.json
    → Streamlit Chat UI
```

## Tech Stack

| Component | Technology |
|---|---|
| Agent Orchestration | LangGraph |
| LLM | Anthropic Claude (claude-sonnet-4) |
| Vector Memory | ChromaDB |
| Data Processing | pandas |
| Chat Interface | Streamlit |
| Language | Python 3.14 |

## Project Structure
```
CRIA/
├── tools.py              # Core credit risk tools (load, flag, narrate)
├── memory.py             # ChromaDB vector memory layer
├── agent.py              # Anomaly review agent
├── reg_agent.py          # Regulatory prep agent
├── orchestrator.py       # Multi-agent orchestrator
├── app.py                # Streamlit chat interface
├── create_data.py        # Synthetic portfolio data generator
└── README.md
```

## How to Run

**Install dependencies:**
```bash
pip install anthropic langchain langchain-anthropic langgraph chromadb streamlit python-dotenv pandas
```

**Set your API key** in a `.env` file:
```
ANTHROPIC_API_KEY=your-key-here
```

**Generate synthetic portfolio data:**
```bash
python create_data.py
```

**Run the full multi-agent pipeline:**
```bash
python orchestrator.py
```

**Launch the chat interface:**
```bash
python -m streamlit run app.py
```

## Context

Built as an independent side project to develop applied AI engineering skills 
relevant to credit risk analytics in regulated financial institutions. All data 
is synthetically generated — no real customer or institutional data is used.

The architecture mirrors patterns emerging in enterprise AI deployments: 
tool-augmented agents, persistent vector memory, multi-agent coordination, 
and human-in-the-loop review workflows.
```

**Step 4: Push to GitHub**

Go to [github.com](https://github.com) and create a new repository called `CRIA`. Then run these commands in your terminal one at a time:
```
git init
git add .
git commit -m "Initial commit — CRIA multi-agent credit risk system"
git branch -M main
git remote add origin https://github.com/hockey408/CRIA.git
git push -u origin main