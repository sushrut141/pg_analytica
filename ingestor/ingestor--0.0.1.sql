--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ingestor;" to load this file. \quit

-- Launch an ingestion worker to create columnar store for the following table
-- sorted by the supplied keys. Both table_name and keys are required arguments.
CREATE FUNCTION ingestor_launch(tablename text, keys text[])
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;
