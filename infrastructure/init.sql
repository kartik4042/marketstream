-- ============================================================
-- MarketStream Platform - Database Schema
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        VARCHAR(64)  UNIQUE NOT NULL,
    user_id         VARCHAR(64)  NOT NULL,
    item_id         VARCHAR(64)  NOT NULL,
    quantity        INTEGER      NOT NULL CHECK (quantity > 0),
    price           DECIMAL(12,2) NOT NULL,
    status          VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_user_id  ON orders(user_id);
CREATE INDEX idx_orders_status   ON orders(status);
CREATE INDEX idx_orders_item_id  ON orders(item_id);

-- Inventory table
CREATE TABLE IF NOT EXISTS inventory (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    item_id      VARCHAR(64)   UNIQUE NOT NULL,
    item_name    VARCHAR(255)  NOT NULL,
    quantity     INTEGER       NOT NULL CHECK (quantity >= 0),
    reserved     INTEGER       NOT NULL DEFAULT 0,
    price        DECIMAL(12,2) NOT NULL,
    updated_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- Payments table
CREATE TABLE IF NOT EXISTS payments (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payment_id      VARCHAR(64)   UNIQUE NOT NULL,
    order_id        VARCHAR(64)   NOT NULL,
    user_id         VARCHAR(64)   NOT NULL,
    amount          DECIMAL(12,2) NOT NULL,
    status          VARCHAR(32)   NOT NULL DEFAULT 'PENDING',
    idempotency_key VARCHAR(128)  UNIQUE NOT NULL,
    processed_at    TIMESTAMP,
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_payments_status   ON payments(status);

-- Dead letter queue audit log
CREATE TABLE IF NOT EXISTS dlq_events (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic        VARCHAR(128) NOT NULL,
    partition    INTEGER,
    offset_val   BIGINT,
    key          VARCHAR(255),
    payload      TEXT,
    error_msg    TEXT,
    retry_count  INTEGER DEFAULT 0,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Processed events for idempotency tracking
CREATE TABLE IF NOT EXISTS processed_events (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    event_type      VARCHAR(64),
    processed_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed inventory data
INSERT INTO inventory (item_id, item_name, quantity, price) VALUES
    ('ITEM-001', 'Vintage Rolex Watch',     10,  4999.99),
    ('ITEM-002', 'iPhone 15 Pro Max',       50,   999.99),
    ('ITEM-003', 'Nike Air Jordan 1',       200,   189.99),
    ('ITEM-004', 'MacBook Pro M3',          25,  2499.99),
    ('ITEM-005', 'PlayStation 5',           15,   499.99),
    ('ITEM-006', 'Dyson V15 Vacuum',        40,   749.99),
    ('ITEM-007', 'Sony WH-1000XM5',        100,   349.99),
    ('ITEM-008', 'Lego Technic Bugatti',    30,   449.99)
ON CONFLICT (item_id) DO NOTHING;
