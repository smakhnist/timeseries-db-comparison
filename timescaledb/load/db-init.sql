CREATE TABLE trades (
                       symbol      TEXT NOT NULL,
                       timestamp   TIMESTAMPTZ NOT NULL,
                       price       DOUBLE PRECISION,
                       volume      BIGINT,
                       is_buy      BOOLEAN
);

SELECT create_hypertable('trades', 'timestamp', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX ON trades (symbol, timestamp DESC);