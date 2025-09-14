import csv

from collections import defaultdict
from copy import copy


import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from sklearn.metrics.pairwise import cosine_similarity


'''
sf = StocksFeatures.read_csv('../data/sp500_metadata.csv')
# Export company data
sf.export_attrs('../data/co.csv')
dmat = sf.pearson_features()
sf.export_graph(dmat, 'pearson', '../data/pearson.csv')

'''



class StocksFeatures(object):

    Date = 'Date'
    Close = 'Close'
    Ticker = 'Ticker'

    @classmethod
    def read_csv(cls, path='./sp500_metadata.csv'):
        df = pd.read_csv(path)
        df['Date'] = df.Date.map(pd.Timestamp)
        return cls(df)

    def __init__(self, df, periods=1):
        self.df = df
        self._names = []
        self._ticker_idx = {}
        self._nameset = set()
        self.periods = periods
        self.preprocess()

        # Cache matrices
        self._diff_mat = None
        self._pearson = None
        self._cosine = None

    def names(self):
        return copy(self._names)

    def nameset(self):
        return self._nameset

    def ix(self, name):
        return self._ticker_idx[name.upper()]
    def name(self, ix):
        return self._names[ix]

    def preprocess(self):
        self._names = list(sorted(self.df[self.Ticker].unique()))
        for ix, name in enumerate(self._names):
            name = name.upper()
            self._ticker_idx[name] = ix
            self._nameset.add(name)

    def _ts_for_ticker(self, ticker):
        df = self.df.loc[self.df['Ticker'] == ticker][['Date', 'Close']].copy()
        out = []
        for ix, row in df.iterrows():
            out.append((row.Date, row.Close))
        return out

    def ts_for_tickers(self, tickers):
        ts = {}
        for t in tickers:
            ts[t] = self._ts_for_ticker(t)
        return {'ts': ts}

    def diff_matrix(self, row_name='Close', col_name='Ticker', ts_col='Date'):
        '''
        Make a relative difference vector from each time series.
        '''
        if self._diff_mat is None:
            names = sorted(self.df[col_name].unique())
            m = []
            for name in names:
                nf = self.df.loc[self.df[col_name] == name].copy()

                if not nf[ts_col].is_monotonic_increasing:
                    raise Exception('need to sort ts column')

                m.append(nf[row_name].diff(periods=self.periods)[1:])
            # (<num tickers>, <num ts observations>)
            self._diff_mat = np.array(m)

        return self._diff_mat

    def pearson_features(self):
        mat = self.diff_matrix()
        return np.corrcoef(mat)

    def spearman_features(self):
        mat = self.diff_matrix()
        return spearmanr(man, axis=1)

    def pearson(self):
        if self._pearson is None:
            self._pearson = self.pearson_features()
        return self._pearson

    def cosine_features(self):
        if self._cosine is None:
            mat = self.diff_matrix()
            self._cosine = cosine_similarity(mat)
        return self._cosine

    def export_attrs(self, path):
        '''
        Export the company attributes.
        '''
        header = ['ticker', 'name', 'sector', 'industry']
        unique = self.df[['Ticker', 'Short Name', 'Sector', 'Industry']].drop_duplicates()
        with open(path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for _, row in unique.iterrows():
                writer.writerow(row.to_list())



    def export_graph(self, corr_mat, relationship, path, drop_threshold=0,
                     replace_drop=None):
        '''
        Given a correlation matrix, export an undirected graph to CSV.

        '''
        header = ['a', 'b', 'relationship', 'weight']
        with open(path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for a, b in zip(*np.tril_indices(corr_mat.shape[0], k=-1)):
                val = corr_mat[(a, b)]

                skip_or_swap = drop_threshold is not None and val <= drop_threshold
                if skip_or_swap:
                    # Swap
                    val = replace_drop

                if val is None:
                    # Skip
                    continue
                writer.writerow([self.name(a), self.name(b), relationship, val*1000])

    def nearest_corr(self, corr_matrix, name, n=100):
        """
        corr_matrix: numpy matrix of correlations, shape (n, n).
        name: stock ticker, eg. 'AAPL'
        n: top, closest N to return.

        Return list of (<ticker name>, <similarity ranking to target>)
        """
        ix = self.ix(name.upper())
        s = pd.Series(corr_matrix[ix])
        s_sorted = s.nlargest(n+1)[1:]
        out = []
        for ix, rank in s_sorted.items():
            n = self.name(ix)
            out.append((n, rank))
        return out

    def rank_tickers(self, target, others, n):
        '''
        tickers: list of stock tickers
        others: list of stock tikers
        n: select the most `n` most correlated tickers.

        Return list of sorted tickers and their scores.
        '''
        target_ix = self.ix(target)
        p_corr = self.pearson_features()
        target_corr = p_corr[target_ix]

        namea = np.array(self.names())
        nmask = np.isin(namea, others)
        ranks = target_corr[nmask]
        out = []
        for ticker, rank in zip(others, ranks):
            out.append((ticker, rank))
        return sorted(out, reverse=True, key=lambda x: x[1])[:n]

    def ts_ranked(self, ticker, grouped, n):
        '''
        ticker: stock ticker, eg., AAPL
        grouped: output from cls.collect_groups, see below.
        n: top N tickers to return per group

        Returns: API response object for grouped; see
        https://github.com/mids-w205/project-3-fardaevm/issues/7

        '''
        out = []
        for blob in grouped:
            ranked = self.rank_tickers(ticker, blob['similar'], n)
            temp = {
                'group': blob['group'],
                'rank': ranked
            }
            filtered_similar = [_[0] for _ in ranked]
            if 'ticker' in blob:
                temp.update(self.ts_for_tickers(filtered_similar + [ticker]))
                temp['ticker'] = ticker
            else:
                temp.update(self.ts_for_tickers(filtered_similar))

            out.append(temp)
        return out


    @classmethod
    def collect_groups(cls, target, groupings, n):
        '''
        target: stock ticker query, eg., 'GOOGL'
        groupings: [[<group id>, <ticker>], ...]

        Returns: [{'group': <group id>,
        'ticker': <target, if target has this group id>, ## Optional
        'similar': [<ticker>, ...]},
        ...]
        '''
        out = []
        similar = defaultdict(list)
        target_group_id = None

        for group_id, ticker in groupings:
            similar[group_id].append(ticker)
            if ticker == target:
                target_group_id = group_id

        target_blob = {}
        for gid, tickers in similar.items():
            if gid == target_group_id:
                target_blob['ticker'] = target
                target_blob['group'] = gid
                target_blob['similar'] = tickers
                continue
            blob = {}
            blob['group'] = gid
            blob['similar'] = tickers
            out.append(blob)

        return [target_blob] + out
