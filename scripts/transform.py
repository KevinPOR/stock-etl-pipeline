import pandas as pd
from db import get_engine


def extract_data():
    engine = get_engine()

    # Load raw fact data
    fact_df = pd.read_sql("SELECT * FROM stock_data", engine)

    # Load dimension table
    dim_df = pd.read_sql("SELECT * FROM dim_tickers", engine)

    return fact_df, dim_df


def transform_data(fact_df, dim_df):
    # --- Clean raw data ---
    fact_df["datetime"] = pd.to_datetime(fact_df["datetime"])
    fact_df["ingestion_time"] = pd.to_datetime(fact_df["ingestion_time"])

    fact_df = fact_df.drop_duplicates()

    # Sort for time-series calculations
    fact_df = fact_df.sort_values(by=["ticker", "datetime"])

    # --- Rename columns ---
    fact_df = fact_df.rename(columns={
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
        "volume": "volume"
    })

    # --- Create metrics ---
    fact_df["price_change"] = fact_df.groupby("ticker")["close_price"].diff()

    fact_df["price_change_pct"] = (
        fact_df.groupby("ticker")["close_price"].pct_change()
    )

    # Rolling averages
    fact_df["ma_5"] = (
        fact_df.groupby("ticker")["close_price"]
        .rolling(5)
        .mean()
        .reset_index(level=0, drop=True)
    )

    fact_df["ma_10"] = (
        fact_df.groupby("ticker")["close_price"]
        .rolling(10)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Daily high/low
    fact_df["date"] = fact_df["datetime"].dt.date

    fact_df["daily_high"] = fact_df.groupby(
        ["ticker", "date"]
    )["high_price"].transform("max")

    fact_df["daily_low"] = fact_df.groupby(
        ["ticker", "date"]
    )["low_price"].transform("min")

    # --- Join with dimension table ---
    df = fact_df.merge(dim_df, on="ticker", how="left")

    # --- Final column selection ---
    df = df[
        [
            "datetime",
            "ticker",
            "company_name",
            "sector",
            "industry",
            "country",
            "currency",
            "exchange",
            "asset_type",
            "market_cap",
            "isin",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "price_change",
            "price_change_pct",
            "ma_5",
            "ma_10",
            "daily_high",
            "daily_low",
            "ingestion_time"
        ]
    ]

    return df


def load_data(df):
    engine = get_engine()

    df.to_sql(
        "fact_stock_prices",
        engine,
        if_exists="replace",  # change to 'append' later for production
        index=False
    )

    print("fact_stock_prices table created!")


def main():
    fact_df, dim_df = extract_data()
    df_transformed = transform_data(fact_df, dim_df)
    load_data(df_transformed)


if __name__ == "__main__":
    main()