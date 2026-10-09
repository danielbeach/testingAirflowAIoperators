-- Target warehouse database. The "customers" table here has DRIFTED from shop.customers
-- on purpose so the schema-compare DAG has something to find:
--   * id            BIGINT        -> INTEGER          (overflow risk: critical)
--   * email         VARCHAR(255)  -> VARCHAR(100)     (truncation risk: warning)
--   * loyalty_tier                -> missing          (critical: column not in target)
--   * signup_ts     TIMESTAMPTZ   -> TIMESTAMP        (timezone loss: warning)
--   * lifetime_value NUMERIC(12,2) -> NUMERIC(10,2)   (precision loss: warning)
--   * name          VARCHAR(255)  -> TEXT             (compatible: info)
\connect warehouse

CREATE TABLE customers (
    id             INTEGER PRIMARY KEY,
    name           TEXT         NOT NULL,
    email          VARCHAR(100) NOT NULL,
    country        CHAR(2)      NOT NULL,
    signup_ts      TIMESTAMP    NOT NULL,
    lifetime_value NUMERIC(10, 2),
    _loaded_at     TIMESTAMP    DEFAULT now()
);
