-- Creates the DWH database inside the same Postgres instance that holds
-- Airflow metadata. Runs once, on first container init.
CREATE DATABASE dwh;
