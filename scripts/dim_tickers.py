import yfinance as yf
import pandas as pd
from datetime import datetime
from db import get_engine


def fetch_ticker_info(ticker):
    stock = yf.Ticker(ticker)
    info = stock.info

    return {
        "ticker": ticker,
        "company_name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "country": info.get("country"),
        "currency": info.get("currency"),
        "exchange": info.get("exchange"),
        "asset_type": "stock",
        "market_cap": info.get("marketCap"),
        "ipo_date": None,  # Often missing in yfinance
        "isin": info.get("isin"),
        "first_seen_date": datetime.now(),
        "last_updated": datetime.now()
    }


def main():
    tickers = ["AAPL", "MSFT", "TSLA"]

    data = [fetch_ticker_info(t) for t in tickers]
    df = pd.DataFrame(data)

    engine = get_engine()

    df.to_sql(
        "dim_tickers",
        engine,
        if_exists="replace",
        index=False
    )

    print("dim_tickers table created!")


if __name__ == "__main__":
    main()