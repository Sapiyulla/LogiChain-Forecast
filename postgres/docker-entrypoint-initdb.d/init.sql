CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Raw факты
CREATE TABLE bronze.raw_warehouse_occupancy (
    timestamp           TIMESTAMP NOT NULL,
    warehouse_id        INTEGER NOT NULL,
    occupied_pallets    INTEGER,
    receiving_volume_24h INTEGER,
    shipping_volume_24h  INTEGER,
    wms_batch_id        UUID NOT NULL
);

-- Справочник складов
CREATE TABLE bronze.dim_warehouses (
    warehouse_id   INTEGER PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    total_capacity INTEGER NOT NULL,
    address        VARCHAR(200),
    timezone       VARCHAR(50) DEFAULT 'UTC',
    client_type    VARCHAR(50),
    is_active      BOOLEAN DEFAULT TRUE
);

-- Справочник клиентов
CREATE TABLE bronze.dim_clients (
    client_id      SERIAL PRIMARY KEY,
    client_name    VARCHAR(100) NOT NULL,
    industry       VARCHAR(50),
    contract_start DATE,
    contract_end   DATE
);

-- Связь складов и клиентов
CREATE TABLE silver.ref_warehouse_clients (
    warehouse_id INTEGER REFERENCES bronze.dim_warehouses(warehouse_id),
    client_id    INTEGER REFERENCES bronze.dim_clients(client_id),
    valid_from   DATE,
    valid_to     DATE,
    PRIMARY KEY (warehouse_id, client_id)
);

-- Календарь
CREATE TABLE silver.dim_calendar (
    date_id        DATE PRIMARY KEY,
    year           INTEGER,
    month          INTEGER,
    day            INTEGER,
    day_of_week    INTEGER,
    is_weekend     BOOLEAN,
    is_holiday     BOOLEAN,
    holiday_name   VARCHAR(50),
    event_type     VARCHAR(50),
    days_to_black_friday INTEGER
);