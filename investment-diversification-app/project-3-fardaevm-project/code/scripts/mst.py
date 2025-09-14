import pandas as pd
import numpy as np
from scipy.sparse.csgraph import minimum_spanning_tree

# Step 1: Load time series from sp500_metadata.csv #
df = pd.read_csv("../data/sp500_metadata.csv")  # assumed to have 'Date', 'Ticker', 'Close'

# Pivot: rows = dates, columns = tickers, values = prices
price_df = df.pivot(index='Date', columns='Ticker', values='Close').sort_index()

# Drop tickers with missing values
price_df = price_df.dropna(axis=1)

# Step 2: Calculate log returns #
returns = np.log(price_df / price_df.shift(1)).dropna()

# Step 3: Compute Pearson correlation matrix #
corr_matrix = returns.corr()

# Step 4: Convert to distance matrix #
distance_matrix = 2 * (1 - corr_matrix)
np.fill_diagonal(distance_matrix.values, np.inf)  # prevent self-loops

# Step 5: Apply Minimum Spanning Tree (MST) #
mst = minimum_spanning_tree(distance_matrix.values).toarray()

# Step 6: Convert MST back to correlation edges #
tickers = corr_matrix.columns.to_list()
edges = []

for i in range(len(tickers)):
    for j in range(len(tickers)):
        if mst[i, j] > 0:
            corr = 1 - (mst[i, j] / 2)
            edges.append({
                'a': tickers[i],
                'b': tickers[j],
                'weight': corr,
                'relationship': 'MST'
            })

edges_df = pd.DataFrame(edges)
edges_df.to_csv("../data/pearson_mst.csv", index=False)

print(f"Created MST edge list with {len(edges)} edges → saved to data/pearson_mst.csv")
