--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ingestor;" to load this file. \quit

-- Launch an ingestion worker to create columnar store for the following table
-- sorted by the supplied columns. Both table_name and keys are required arguments.
CREATE FUNCTION ingestor_launch(tablename text, columns text[])
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;


CREATE FUNCTION register_export()
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C;
