# ingestion.py: Fetches daily stock data from Yahoo Finance API
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os


def fetch_data(ticker):
    try:
        print(f"Fetching data for {ticker}...")

        # 8-day range
        end_date = datetime.today()
        start_date = end_date - timedelta(days=8)

        print(f"Downloading 1-minute data for {ticker} from {start_date.date()} to {end_date.date()}...")
        df = yf.download(ticker, start=start_date, end=end_date, interval="1m")

        if df.empty:
            print(f"No data returned for {ticker}")
            return None

        df.reset_index(inplace=True)

        # 1. Standardize columns IMMEDIATELY
        # This removes the '_aapl' or '_msft' suffix from yfinance MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0].lower().replace(" ", "_") for col in df.columns]
        else:
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]

        df["ticker"] = ticker
        df["ingestion_time"] = datetime.now()


        return df

    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None


def save_data(df, ticker):
    try:
        os.makedirs("data/raw", exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"data/raw/{ticker}_{timestamp}.csv"

        df.to_csv(path, index=False)
        print(f"Saved {ticker} data to {path}")

    except Exception as e:
        print(f"Error saving {ticker}: {e}")


def main():
    tickers = ["AAPL", "MSFT", "TSLA"]

    for ticker in tickers:
        df = fetch_data(ticker)

        if df is not None:
            save_data(df, ticker)


if __name__ == "__main__":
    main()