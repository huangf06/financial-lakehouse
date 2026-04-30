SELECT symbol, count(*) AS trade_count
FROM silver.trades
GROUP BY symbol;
