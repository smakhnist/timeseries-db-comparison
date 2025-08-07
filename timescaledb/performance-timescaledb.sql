-- check size --
SELECT COUNT(*) FROM trades;

-- ohlcv query for AAPL trades --
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    first(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, timestamp) AS close,
    sum(volume) AS volume
FROM trades
WHERE timestamp >= '2025-02-01' AND timestamp < '2025-02-04'
  AND symbol = 'AAPL'
GROUP BY bucket