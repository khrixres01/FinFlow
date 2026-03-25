CREATE DATABASE IF NOT EXISTS finflow
COMMENT 'FinFlow Financial Transaction Warehouse';

USE finflow;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id       STRING,
    full_date     DATE,
    day           INT,
    month         INT,
    year          INT,
    quarter       INT,
    day_of_week   STRING,
    is_weekend    BOOLEAN,
    is_holiday    BOOLEAN
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id   STRING,
    full_name     STRING,
    age           INT,
    city          STRING,
    account_tier  STRING,
    join_date     DATE,
    is_active     BOOLEAN
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_accounts (
    account_id    STRING,
    customer_id   STRING,
    account_type  STRING,
    currency      STRING,
    opened_date   DATE
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id    STRING,
    date_id           STRING,
    customer_id       STRING,
    account_from      STRING,
    account_to        STRING,
    amount            DOUBLE,
    transaction_type  STRING,
    channel           STRING,
    status            STRING,
    is_flagged        BOOLEAN,
    flag_reason       STRING,
    processed_at      TIMESTAMP,
    transaction_hour  INT
)
PARTITIONED BY (transaction_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS agg_daily_summary (
    summary_date          DATE,
    total_transactions    INT,
    total_volume          DOUBLE,
    avg_transaction       DOUBLE,
    max_transaction       DOUBLE,
    successful_count      INT,
    failed_count          INT,
    flagged_count         INT,
    flagged_volume        DOUBLE
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS stg_transactions (
    transaction_id    STRING,
    txn_timestamp         TIMESTAMP,
    customer_id       STRING,
    account_from      STRING,
    account_to        STRING,
    amount            DOUBLE,
    transaction_type  STRING,
    channel           STRING,
    status            STRING,
    city              STRING,
    processed_at      TIMESTAMP,
    transaction_date  DATE,
    transaction_hour  INT,
    is_flagged        BOOLEAN,
    flag_reason       STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/staging/flagged/';

SHOW TABLES;