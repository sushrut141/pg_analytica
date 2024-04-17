--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_analytica" to load this file. \quit

-- CREATE OR REPLACE FUNCTION get_sum(int, int) RETURNS int
-- AS '$libdir/get_sum'
-- LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pg_analytica_tableam_handler(internal)
RETURNS table_am_handler AS 'pg_analytica', 'pg_analytica_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD pg_analytica_am TYPE TABLE HANDLER pg_analytica_tableam_handler;