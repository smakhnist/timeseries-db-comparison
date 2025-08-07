CREATE TABLE trades
(
    timestamp    DateTime64(3),         -- millisecond precision
    symbol  String,
    price   Float64,
    volume  Float64,
    is_buy  UInt8                -- optional, can be used to filter buy/sell trades
)
    ENGINE = MergeTree
        PARTITION BY toDate(timestamp)          -- partitions by day
        ORDER BY (symbol, timestamp)            -- enables fast range filtering
        SETTINGS index_granularity = 8192;