from dotenv import load_dotenv
load_dotenv()

import chromadb
import json
import os
from datetime import datetime

# Initialize a local ChromaDB database — this creates a folder called 'cria_memory'
client = chromadb.PersistentClient(path="./cria_memory")

# Create a collection — think of this like a table in your database
collection = client.get_or_create_collection(
    name="anomaly_reviews",
    metadata={"description": "Credit risk anomaly review history"}
)


def save_review(account_data: dict, narrative: str) -> None:
    """
    Saves a completed anomaly review to persistent memory.
    Uses account_id + timestamp as unique identifier.
    """
    review_id = f"{account_data['account_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # The document is what gets searched — make it rich
    document = f"""
    Account: {account_data['account_id']}
    Segment: {account_data['segment']}
    Z-Score: {account_data['z_score']}
    Severity: {account_data.get('severity', 'N/A')}
    Rating: {account_data['rating']}
    Balance: {account_data['outstanding_balance']}
    Narrative: {narrative}
    """
    
    # Metadata is filterable — useful for queries like "show me all CRE reviews"
    metadata = {
        "account_id": account_data["account_id"],
        "segment": account_data["segment"],
        "z_score": str(account_data["z_score"]),
        "rating": account_data["rating"],
        "review_date": datetime.now().strftime("%Y-%m-%d"),
        "narrative": narrative
    }
    
    collection.add(
        documents=[document],
        metadatas=[metadata],
        ids=[review_id]
    )
    print(f"Saved review for {account_data['account_id']} to memory")


def search_reviews(query: str, n_results: int = 3) -> list:
    """
    Searches past reviews using semantic similarity.
    Example: search_reviews("high severity CRE accounts") returns relevant past reviews.
    """
    results = collection.query(
        query_texts=[query],
        n_results=min(n_results, collection.count()) if collection.count() > 0 else 1
    )
    
    if not results["metadatas"][0]:
        return []
    
    reviews = []
    for metadata in results["metadatas"][0]:
        reviews.append(metadata)
    
    return reviews


def get_review_history(account_id: str) -> list:
    """
    Retrieves all past reviews for a specific account.
    """
    results = collection.get(
        where={"account_id": account_id}
    )
    
    if not results["metadatas"]:
        return []
    
    return results["metadatas"]


def get_memory_stats() -> dict:
    """
    Returns summary statistics about what's in memory.
    """
    count = collection.count()
    return {
        "total_reviews": count,
        "database_path": "./cria_memory"
    }