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

CREATE TABLE IF NOT EXISTS data.invalid_products (
    id SERIAL PRIMARY KEY,
    sku INTEGER NOT NULL,
    name VARCHAR(255),
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(255) NOT NULL,
    popularity DECIMAL(5, 4) NOT NULL,
    error_message TEXT,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.invalid_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.invalid_customers (
    id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);