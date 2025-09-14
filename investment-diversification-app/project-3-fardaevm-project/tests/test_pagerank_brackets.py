#!/usr/bin/env python3
import os
from scripts.graph import Session

# For testing purposes, hard-code the password.
os.environ["N4J_PW"] = "test1234"

def test_personalized_pagerank_groups(ticker="AAPL"):
    sess = Session(ip="localhost", db="neo4j")
    print(f"\n Running Personalized PageRank for: {ticker}...")
    try:
        sess.run_personalized_pagerank(source_ticker=ticker)
    except Exception as e:
        print(f" Error executing personalized PageRank: {e}")
        return

    # Get all similar tickers (excluding the source ticker) sorted by descending personalizedPageRank
    all_results = sess.get_all_similar_by_ppr(ticker)

    if not all_results:
        print("No similar tickers found.")
        return

    total = len(all_results)
    # Calculate indices to split the list into three nearly equal groups.
    top_threshold = total // 3
    bottom_threshold = 2 * total // 3

    top_results = all_results[:top_threshold]
    middle_results = all_results[top_threshold:bottom_threshold]
    bottom_results = all_results[bottom_threshold:]

    # For top and middle group, restrict to top 5 entries. For bottom, get bottom 5 entries.
    top_group = top_results[:5]
    middle_group = middle_results[:5]
    bottom_group = bottom_results[-5:]

    # Create a dictionary to store these groups.
    groups = {
        "top": top_group,
        "middle": middle_group,
        "bottom": bottom_group,
    }

    print(f"\n Top group (highest personalized PageRank, top 5):")
    for record in top_group:
        print(f"   - {record['ticker']}: {record['score']:.4f}")

    print(f"\n Middle group (top middle 5):")
    for record in middle_group:
        print(f"   - {record['ticker']}: {record['score']:.4f}")

    print(f"\n Bottom group (lowest personalized PageRank, bottom 5):")
    for record in bottom_group:
        print(f"   - {record['ticker']}: {record['score']:.4f}")

    return groups

if __name__ == "__main__":
    groups = test_personalized_pagerank_groups("AAPL")
