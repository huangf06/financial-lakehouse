SELECT date_trunc('hour', event_ts) AS hour, sum(notional) / sum(quantity) AS vwap
FROM silver.trades
WHERE symbol = 'BTCUSDT'
  AND event_date >= current_date() - INTERVAL 7 DAYS
GROUP BY 1;
