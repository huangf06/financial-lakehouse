SELECT symbol, sum(quantity) AS volume
FROM silver.trades
WHERE symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'AAPL', 'MSFT')
  AND event_ts >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY symbol;
