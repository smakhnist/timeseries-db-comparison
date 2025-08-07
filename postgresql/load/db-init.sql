CREATE TABLE trades
(
    timestamp TIMESTAMPTZ    NOT NULL,
    symbol    VARCHAR(20)    NOT NULL,
    price     NUMERIC(18, 8) NOT NULL,
    volume    INTEGER        NOT NULL,
    is_buy      BOOLEAN        NOT NULL
);

-- Composite index for symbol + timestamp (common query pattern)
CREATE INDEX idx_trades_symbol_timestamp ON trades(symbol, timestamp);