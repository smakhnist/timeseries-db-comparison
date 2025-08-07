SELECT
    timestamp,
    first(price) AS open,
    max(price)   AS high,
    min(price)   AS low,
    last(price)  AS close,
    sum(volume)  AS volume
FROM trades
WHERE symbol = 'AAPL'
  AND timestamp BETWEEN '2025-02-01T00:00:00.000Z' AND '2025-02-03T23:59:59.999Z'
    SAMPLE BY 1m ALIGN TO CALENDAR;