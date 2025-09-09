-- Run automatically on first Postgres start (mounted by compose)

-- Create schemas in dwh
\connect dwh

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

-- RAW: store each row as JSONB + yyyymm for easy incremental slicing
CREATE TABLE IF NOT EXISTS raw.operator_cycle (
  yyyymm INTEGER NOT NULL,
  raw JSONB NOT NULL,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_raw_operator_cycle_yyyymm ON raw.operator_cycle(yyyymm);

CREATE TABLE IF NOT EXISTS raw.lease_cycle (
  yyyymm INTEGER NOT NULL,
  raw JSONB NOT NULL,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_raw_lease_cycle_yyyymm ON raw.lease_cycle(yyyymm);

-- STAGING: typed, normalized monthlies
CREATE TABLE IF NOT EXISTS staging.operator_monthly (
  operator_no INTEGER NOT NULL,
  operator_name TEXT,
  yyyymm INTEGER NOT NULL,
  oil_bbl NUMERIC(18,2) DEFAULT 0 CHECK (oil_bbl >= 0),
  gas_mcf NUMERIC(18,2) DEFAULT 0 CHECK (gas_mcf >= 0),
  cond_bbl NUMERIC(18,2) DEFAULT 0 CHECK (cond_bbl >= 0),
  csgd_mcf NUMERIC(18,2) DEFAULT 0 CHECK (csgd_mcf >= 0),
  PRIMARY KEY (operator_no, yyyymm)
);

CREATE TABLE IF NOT EXISTS staging.lease_monthly (
  operator_no INTEGER,
  district_no INTEGER,
  field_no INTEGER,
  lease_no INTEGER,
  lease_name TEXT,
  lease_key TEXT NOT NULL,           -- district-lease combo for uniqueness
  yyyymm INTEGER NOT NULL,
  oil_bbl NUMERIC(18,2) DEFAULT 0 CHECK (oil_bbl >= 0),
  gas_mcf NUMERIC(18,2) DEFAULT 0 CHECK (gas_mcf >= 0),
  cond_bbl NUMERIC(18,2) DEFAULT 0 CHECK (cond_bbl >= 0),
  csgd_mcf NUMERIC(18,2) DEFAULT 0 CHECK (csgd_mcf >= 0),
  PRIMARY KEY (lease_key, yyyymm)
);

-- CURATED: simple dims + facts (natural keys for simplicity)
CREATE TABLE IF NOT EXISTS curated.dim_operator (
  operator_no INTEGER PRIMARY KEY,
  operator_name TEXT
);

CREATE TABLE IF NOT EXISTS curated.dim_district (
  district_no INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS curated.dim_field (
  field_no INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS curated.dim_lease (
  lease_key TEXT PRIMARY KEY,
  operator_no INTEGER,
  district_no INTEGER,
  field_no INTEGER,
  lease_no INTEGER,
  lease_name TEXT
);

CREATE TABLE IF NOT EXISTS curated.fact_operator_monthly (
  operator_no INTEGER NOT NULL,
  yyyymm INTEGER NOT NULL,
  oil_bbl NUMERIC(18,2) DEFAULT 0 CHECK (oil_bbl >= 0),
  gas_mcf NUMERIC(18,2) DEFAULT 0 CHECK (gas_mcf >= 0),
  cond_bbl NUMERIC(18,2) DEFAULT 0 CHECK (cond_bbl >= 0),
  csgd_mcf NUMERIC(18,2) DEFAULT 0 CHECK (csgd_mcf >= 0),
  PRIMARY KEY (operator_no, yyyymm)
);

CREATE TABLE IF NOT EXISTS curated.fact_lease_monthly (
  lease_key TEXT NOT NULL,
  yyyymm INTEGER NOT NULL,
  oil_bbl NUMERIC(18,2) DEFAULT 0 CHECK (oil_bbl >= 0),
  gas_mcf NUMERIC(18,2) DEFAULT 0 CHECK (gas_mcf >= 0),
  cond_bbl NUMERIC(18,2) DEFAULT 0 CHECK (cond_bbl >= 0),
  csgd_mcf NUMERIC(18,2) DEFAULT 0 CHECK (csgd_mcf >= 0),
  PRIMARY KEY (lease_key, yyyymm)
);
