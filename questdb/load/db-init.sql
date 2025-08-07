CREATE TABLE trades (
                        timestamp TIMESTAMP,
                        symbol SYMBOL,           -- indexed automatically
                        price DOUBLE,
                        volume INT,
                        is_buy BOOLEAN
) timestamp(timestamp);