-- Source OLTP database ("shop"): the system of record.
CREATE DATABASE shop;
CREATE DATABASE warehouse;

\connect shop

CREATE TABLE customers (
    id            BIGINT PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    email         VARCHAR(255) NOT NULL UNIQUE,
    country       CHAR(2)      NOT NULL,
    loyalty_tier  VARCHAR(20),
    signup_ts     TIMESTAMPTZ  NOT NULL,
    lifetime_value NUMERIC(12, 2) DEFAULT 0
);

CREATE TABLE orders (
    id           BIGINT PRIMARY KEY,
    customer_id  BIGINT       NOT NULL REFERENCES customers (id),
    status       VARCHAR(20)  NOT NULL,
    total        NUMERIC(12, 2) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL
);

CREATE TABLE order_items (
    order_id    BIGINT        NOT NULL REFERENCES orders (id),
    sku         VARCHAR(32)   NOT NULL,
    product     VARCHAR(255)  NOT NULL,
    category    VARCHAR(64)   NOT NULL,
    quantity    INT           NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (order_id, sku)
);

INSERT INTO customers VALUES
 (1, 'Ada Lovelace',     'ada@example.com',     'GB', 'gold',   '2024-01-05 10:00+00', 1840.50),
 (2, 'Grace Hopper',     'grace@example.com',   'US', 'gold',   '2024-02-11 14:30+00', 2210.00),
 (3, 'Alan Turing',      'alan@example.com',    'GB', 'silver', '2024-03-20 09:15+00',  640.25),
 (4, 'Katherine Johnson','kj@example.com',      'US', NULL,     '2024-05-02 16:45+00',  120.00),
 (5, 'Linus Torvalds',   'linus@example.com',   'FI', 'silver', '2024-06-18 08:05+00',  910.75),
 (6, 'Margaret Hamilton','mh@example.com',      'US', 'gold',   '2024-07-01 12:00+00', 3105.40),
 (7, 'Edsger Dijkstra',  'edsger@example.com',  'NL', NULL,     '2024-09-09 19:20+00',   45.00),
 (8, 'Barbara Liskov',   'barbara@example.com', 'US', 'silver', '2025-01-14 11:11+00',  780.00);

INSERT INTO orders VALUES
 (101, 1, 'shipped',   420.50, '2025-07-02 10:00+00'),
 (102, 2, 'shipped',   990.00, '2025-07-03 11:00+00'),
 (103, 6, 'shipped',  1505.40, '2025-07-05 09:30+00'),
 (104, 3, 'cancelled', 140.25, '2025-07-08 15:00+00'),
 (105, 5, 'shipped',   310.75, '2025-08-01 13:45+00'),
 (106, 1, 'returned',  220.00, '2025-08-04 17:10+00'),
 (107, 2, 'shipped',   600.00, '2025-08-15 08:00+00'),
 (108, 6, 'shipped',   800.00, '2025-09-01 20:00+00'),
 (109, 8, 'pending',   380.00, '2025-09-20 10:30+00'),
 (110, 4, 'shipped',   120.00, '2025-09-22 12:00+00'),
 (111, 6, 'shipped',   800.00, '2025-09-28 14:00+00'),
 (112, 7, 'cancelled',  45.00, '2025-10-01 09:00+00');

INSERT INTO order_items VALUES
 (101, 'KB-01',  'Mechanical Keyboard', 'peripherals', 2, 150.25),
 (101, 'MS-02',  'Wireless Mouse',      'peripherals', 2,  60.00),
 (102, 'MN-27',  '27in Monitor',        'displays',    2, 495.00),
 (103, 'LT-14',  'Laptop 14in',         'computers',   1, 1505.40),
 (104, 'HD-01',  'USB-C Hub',           'accessories', 3,  46.75),
 (105, 'HP-11',  'Headphones',          'audio',       1, 310.75),
 (106, 'KB-01',  'Mechanical Keyboard', 'peripherals', 1, 220.00),
 (107, 'MN-27',  '27in Monitor',        'displays',    1, 600.00),
 (108, 'DK-05',  'Standing Desk',       'furniture',   1, 800.00),
 (109, 'CH-02',  'Ergonomic Chair',     'furniture',   1, 380.00),
 (110, 'HD-01',  'USB-C Hub',           'accessories', 2,  60.00),
 (111, 'DK-05',  'Standing Desk',       'furniture',   1, 800.00),
 (112, 'MS-02',  'Wireless Mouse',      'peripherals', 1,  45.00);

-- Least-privilege role for LLM-facing tasks (agent + generated SQL).
CREATE ROLE llm_reader LOGIN PASSWORD 'llm_reader';
GRANT CONNECT ON DATABASE shop TO llm_reader;
GRANT USAGE ON SCHEMA public TO llm_reader;
GRANT SELECT ON customers, orders, order_items TO llm_reader;
