-- check size --
SELECT COUNT(*) FROM trades;

SELECT DISTINCT
    bucket AS interval,
    FIRST_VALUE(price) OVER (PARTITION BY bucket) AS open,
    MAX(price) OVER (PARTITION BY bucket) AS high,
    MIN(price) OVER (PARTITION BY bucket) AS low,
    LAST_VALUE(price) OVER (PARTITION BY bucket) AS close,
    SUM(volume) OVER (PARTITION BY bucket) AS volume
FROM (
         SELECT
             price,
             volume,
             timestamp,
             DATE_TRUNC('minute', timestamp) AS bucket
         FROM trades
         WHERE symbol = 'AAPL' AND
               timestamp BETWEEN '2025-02-01 00:00:00' AND '2025-02-04 00:00:00'
     ) t