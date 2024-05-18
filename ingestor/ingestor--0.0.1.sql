--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ingestor;" to load this file. \quit

-- Table to store export metadata
CREATE TABLE analytica_exports (
    table_name    text PRIMARY KEY,
    last_run_completed timestamp,
    columns_to_export text[],
    export_frequency_hours int,
    export_status int
);

-- Launch an ingestion worker to create columnar store for the following table
-- sorted by the supplied columns. Both table_name and keys are required arguments.
CREATE OR REPLACE FUNCTION ingestor_launch(tablename text, columns text[])
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Register a postgres table for export
CREATE OR REPLACE FUNCTION register_table_export(table_name text, columns_to_export text[], export_frequency_hours int)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

-- Un-register a table for export and delete columnar data directory.
CREATE OR REPLACE FUNCTION unregister_Table_export(table_name text)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;
