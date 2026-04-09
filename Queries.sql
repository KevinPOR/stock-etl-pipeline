SELECT * FROM stock_data LIMIT 5


SELECT count(*) FROM stock_data
GROUP BY ticker


SELECT * FROM dim_tickers LIMIT 5

SELECT * FROM fact_stock_prices LIMIT 5


ALTER TABLE dim_tickers
ADD PRIMARY KEY (ticker);

ALTER TABLE fact_stock_prices
ADD CONSTRAINT fk_ticker
FOREIGN KEY (ticker)
REFERENCES dim_tickers(ticker);


SELECT COUNT(*) FROM stock_data;
SELECT COUNT(*) FROM dim_tickers;
SELECT COUNT(*) FROM fact_stock_prices;


SELECT * FROM fact_stock_prices LIMIT 5;

select columns from fact_stock_prices;


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_tickers'
ORDER BY ordinal_position;
