SELECT *
FROM silver.trades
WHERE symbol = 'BTCUSDT'
  AND event_date >= current_date() - INTERVAL 1 DAY;
