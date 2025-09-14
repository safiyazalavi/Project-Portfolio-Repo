
import os
import sys
from datetime import datetime
from threading import Lock

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    make_response,
    redirect,
)
import pandas as pd

sys.path.append('../scripts')
from features import StocksFeatures
from graph import (
    Session as nSession
)

## Constants
PAGE_RANK = 'page_rank'
LOUVAIN = 'louvain'
LEIDEN = 'leiden'

ALGO_2_PROPERTY = {
    PAGE_RANK: 'pageRank',
    LOUVAIN: 'community',
    LEIDEN: 'leiden_community',
}


## Set up some objects before starting the app.
TS_PATH = '../../data/current.csv'
FEATURES = StocksFeatures.read_csv(TS_PATH)

# Cache the pearson features.
FEATURES.pearson()

## Flask application logic.
app = Flask(__name__)

# Create default route that redirects to ticker
@app.route("/")
def default():
    return redirect("/GOOGL")

@app.route("/<string:ticker>")
def ticker(ticker):
    ticker = ticker.upper()

    with Lock():
         tickers = FEATURES.df['Ticker'].unique()

    graph_algo = request.args.get('a')
    n = int(request.args.get('n', 4))

    data = None

    if graph_algo is not None and "_G" in graph_algo:
        # Call groups on algorithm

        graph_algo = graph_algo[:-2]

        data = api_ticker_groups(ticker, graph_algo, n).get_json()
    else:
        data = api_ticker_similar(ticker, graph_algo, n).get_json()

    if not data:
        return

    if isinstance(data, dict):
        data = [data]

    if 'group' in data[0]:
        groups_data = [group['ts'] for group in data]
        time_series_data = [[{label : format_list_of_timeseries(time_series)} for label, time_series in group.items()] for group in groups_data]
        timestamp_dates = [entry[0] for entry in list(time_series_data[0][0].values())[0]]
        return render_template('main.html', ticker=ticker, labels=timestamp_dates, time_series_data=time_series_data, ticker_options=tickers, group_flag=True)

    else:
        data = data[0]
        time_series_data = [{label : format_list_of_timeseries(time_series)} for label, time_series in data['ts'].items()]
        timestamp_dates = [format_timeseries(entry[0]) for entry in list(data['ts'].items())[0][1]]

        return render_template('main.html', ticker=ticker, labels=timestamp_dates, time_series_data=time_series_data, ticker_options=tickers, group_flag=False)

@app.route("/stonks")
def stonks():
    df = None
    with Lock():
        names = FEATURES.names()
        df = pd.DataFrame(FEATURES.pearson(), columns=names, index=names)

    return render_template('stonks.html', df=df)


@app.route("/communities")
def communities():
    n4j_sess = nSession(os.environ['N4J_IP'])
    res = n4j_sess.get_community()

    return render_template('communities.html', header=['Ticker', 'Community ID'],
                           values=res.values())

#### API endpoints

@app.route("/api/<string:ticker>/similar")
def api_ticker_similar(ticker, algo = None, n = 0):
    graph_algo = algo

    if graph_algo is None:
        graph_algo = request.args.get('a')

    N = int(n)

    if N == 0:
        N = int(request.args.get('n', 6))

    error = None
    ticker = ticker.upper()
    ts = None
    rank = None

    graph_algo = process_algo_name(graph_algo)

    if graph_algo not in ALGO_2_PROPERTY:
        # TODO: maybe return an error instead
        graph_algo = LOUVAIN

    with Lock():
        if ticker not in FEATURES.nameset():
            error = 'unknown stock ticker'

    if error:
        return make_response(jsonify({'error': error}), 400)

    s = nSession(os.environ.get('N4J_IP', None))

    if graph_algo == PAGE_RANK:
        _similar = s.run_personalized_pagerank(ticker)
        rank = [[_.get('ticker'), _.get('score')] for _ in _similar if _.get('score')]
        filtered_similar = [_.get('ticker') for _ in _similar if _.get('score')][:N]
        with Lock():
            # Timeseries data
            ts = FEATURES.ts_for_tickers(filtered_similar + [ticker])
        out = {
            'ticker': ticker,
            'rank': [],
        }
        out.update(ts)

        return make_response(jsonify(out), 200)

    else:
        # Otherwise use the community-based similarity based on a property.
        prop = ALGO_2_PROPERTY[graph_algo]
        similar = s.get_similar(ticker, nproperty=prop)

    with Lock():
        # Rank similar stocks, and trim to top N
        rank = FEATURES.rank_tickers(ticker, similar, N)
    filtered_similar = [_[0] for _ in rank]
    with Lock():
        # Timeseries data
        ts = FEATURES.ts_for_tickers(filtered_similar + [ticker])
    out = {
        'ticker': ticker,
        'rank': rank,
    }
    out.update(ts)

    return make_response(jsonify(out), 200)

@app.route("/api/<string:ticker>/groups")
def api_ticker_groups(ticker, algo = None, n = 0):
    graph_algo = algo

    if graph_algo is None:
        graph_algo = request.args.get('a')

    N = n

    if N == 0:
        N = int(request.args.get('n', 6))

    error = None
    ticker = ticker.upper()

    graph_algo = process_algo_name(graph_algo)

    if graph_algo not in ALGO_2_PROPERTY:
        # TODO: maybe return an error instead
        graph_algo = LOUVAIN
    with Lock():
        if ticker not in FEATURES.nameset():
            error = 'unknown stock ticker'
    if error:
        return make_response(jsonify({'error': error}), 400)

    s = nSession(os.environ.get('N4J_IP', None))
    prop = ALGO_2_PROPERTY[graph_algo]
    groups = s.get_groups(ticker, nproperty=prop)
    collected = FEATURES.collect_groups(ticker, groups, N)
    with Lock():
        ranked_ts = FEATURES.ts_ranked(ticker, collected, N)
    return make_response(jsonify(ranked_ts), 200)

# So default HTTP call for favicon does not interfere with default route request parameters
@app.route('/favicon.ico')
def favicon():
    return '', 200

#### Helper Functions

def format_list_of_timeseries(timestamps):
    return [[format_timeseries(entry[0]), entry[1]] for entry in timestamps]

def format_timeseries(timestamp):
    incoming_format = "%a, %d %b %Y %H:%M:%S GMT"
    desired_format = "%m/%d/%y"
    return datetime.strftime(datetime.strptime(timestamp, incoming_format), desired_format)

def process_algo_name(algo):
    if algo is not None:
        return algo.lower()

    return algo
