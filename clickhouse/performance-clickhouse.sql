-- check size --
SELECT COUNT(*) FROM trades;

-- ohlcv query for AAPL trades in ClickHouse -
SELECT
    symbol,
    toStartOfFiveMinute(timestamp) AS bucket,
    anyHeavy(price) AS open,
    max(price) AS high,
    min(price) AS low,
    anyLast(price) AS close,
    sum(volume) AS volume
FROM trades
WHERE timestamp >= toDateTime('2025-01-01 00:00:00')
  AND timestamp < toDateTime('2025-02-04 00:00:00')
  AND symbol = 'AAPL'
GROUP BY symbol, bucket
ORDER BY bucket