--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ingestor;" to load this file. \quit

-- Table to store export metadata
CREATE TABLE analytica_exports (
    table_name    text PRIMARY KEY,
    last_run_completed TIMESTAMP WITH TIME ZONE,
    columns_to_export text[],
    export_frequency_hours int,
    export_status int,
    chunk_size int
);

-- Register a postgres table for export
CREATE OR REPLACE FUNCTION register_table_export(
    table_name text, 
    columns_to_export text[], 
    export_frequency_hours int, 
    chunk_size int DEFAULT 100000)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

-- Un-register a table for export and delete columnar data directory.
CREATE OR REPLACE FUNCTION unregister_table_export(table_name text)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;


-- Launch an ingestion worker to export columnar data for tables registered for export.
CREATE OR REPLACE FUNCTION ingestor_launch()
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;