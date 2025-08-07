-- check size --
SELECT COUNT(*) FROM trades;

-- ohlcv query for AAPL trades in ClickHouse -
SELECT
    toStartOfMinute(timestamp) AS bucket,
    anyHeavy(price) AS open,
    max(price) AS high,
    min(price) AS low,
    anyLast(price) AS close,
    sum(volume) AS volume
FROM trades
WHERE
    symbol = 'AAPL'
    AND timestamp >= toDateTime('2025-02-01 00:00:00')
  AND timestamp < toDateTime('2025-02-04 00:00:00')
GROUP BY bucket
ORDER BY bucket;