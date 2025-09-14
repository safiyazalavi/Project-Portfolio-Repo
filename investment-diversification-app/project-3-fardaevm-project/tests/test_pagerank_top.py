import os
from graph import Session  # Adjust path if needed (e.g., scripts.graph)

# For testing purposes, you can hard-code the password
os.environ["N4J_PW"] = "ucb_mids_w205"

def test_personalized_pagerank(ticker="A", top_n=5, bottom_n=5):
    sess = Session(ip="18.206.243.97", db="neo4j")
    print(f"\n Running Personalized PageRank for: {ticker}...")
    try:
        # Run personalized PageRank computation; results are stored on each node as 'personalizedPageRank'
        sess.run_personalized_pagerank(source_ticker=ticker)
    except Exception as e:
        print(f" Error executing personalized PageRank: {e}")
        return

    # Get the top similar tickers sorted by descending personalizedPageRank
    top_results = sess.get_top_similar_by_ppr(ticker, top_n)
    print(f"\n Top {top_n} tickers similar to {ticker} (by personalized PageRank):")
    for record in top_results:
        print(f"   - {record['ticker']}: {record['score']:.4f}")

if __name__ == "__main__":
    test_personalized_pagerank("A", top_n=5, bottom_n=5)


