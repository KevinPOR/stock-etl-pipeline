--Insights--

--1. Average Price per Ticker

SELECT ticker, AVG(close_price) AS avg_price
FROM fact_stock_prices
GROUP BY ticker;
ORDER BY avg_price DESC;

--2. Top Volatile Stocks
SELECT ticker, STDDEV(price_change_pct) AS volatility
FROM fact_stock_prices
GROUP BY ticker
ORDER BY volatility DESC;

--3. Daily High vs Low Spread
SELECT ticker, AVG(daily_high - daily_low) AS avg_spread
FROM fact_stock_prices
GROUP BY ticker;

--4. Market Cap Comparison
SELECT ticker, company_name, market_cap
FROM dim_tickers
ORDER BY market_cap DESC;

--5. Daily Spread
SELECT 
    ticker, 
    ROUND(AVG(daily_high - daily_low)::numeric, 4) AS avg_daily_spread
FROM fact_stock_prices
GROUP BY ticker
ORDER BY avg_daily_spread DESC;

--6. Market Cap Comparison
SELECT 
    ticker, 
    company_name, 
    ROUND(market_cap / 1000000000.0, 2) AS market_cap_billion
FROM dim_tickers
ORDER BY market_cap DESC;

--7. Latest Price Snapshot
SELECT DISTINCT ON (ticker)
    ticker,
    datetime,
    close_price
FROM fact_stock_prices
ORDER BY ticker, datetime DESC;