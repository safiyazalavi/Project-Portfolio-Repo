import yfinance as yf
import pandas as pd
from datetime import (
    datetime,
    timedelta,
    UTC
)
import time

STOCKS_DAYS_BACK = 90
YAHOO_SLEEPINESS_SECONDS = 1.2


def get_tickers():

    # Define a list of all tickers in sp500
    wiki_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df_sp500 = pd.read_html(wiki_url)[0]
    df_sp500['Symbol'] = df_sp500['Symbol'].str.replace('.', '-', regex=False)

    tickers = df_sp500['Symbol'].unique().tolist()
    return tickers


def write_stocks(tickers, days_back, path="../data/current.csv"):
    data = []

    today = datetime.now(UTC)
    end_date = today.strftime('%Y-%m-%d')
    start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')

    print("Fetching price and metadata for each stock...")
    for stock in tickers:
        try:
            ticker = yf.Ticker(stock)
            hist = ticker.history(start=start_date, end=end_date)
            if hist.empty:
                continue
            info = ticker.info

            hist['Ticker'] = stock
            hist['Short Name'] = info.get('shortName', '')
            hist['Sector'] = info.get('sector', '')
            hist['Industry'] = info.get('industry', '')

            hist.reset_index(inplace=True)
            data.append(hist)
            time.sleep(YAHOO_SLEEPINESS_SECONDS)

        except Exception as e:
            print(f"Error with {stock}: {e}")

    df_combined = pd.concat(data, ignore_index=True)
    df_combined.to_csv(path, index=False)
    return path

if __name__ == '__main__':
    tickers = get_tickers()
    path = write_stocks(tickers, STOCKS_DAYS_BACK)
    print(f'wrote {path}; done.')
