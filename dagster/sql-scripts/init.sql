CREATE SCHEMA IF NOT EXISTS data;

CREATE TABLE data.processing_statistics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    dataset_type VARCHAR(255) NOT NULL,
    record_count INTEGER NOT NULL,
    processing_time NUMERIC NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.customers (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.products (
    sku INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(50) NOT NULL,
    popularity DOUBLE PRECISION CHECK (popularity > 0) NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);