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
    transaction_id UUID PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);