--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION get_sum" to load this file. \quit

-- CREATE OR REPLACE FUNCTION get_sum(int, int) RETURNS int
-- AS '$libdir/get_sum'
-- LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION custom_tableam_handler(internal)
RETURNS table_am_handler AS 'get_sum', 'custom_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD custom_am TYPE TABLE HANDLER custom_tableam_handler;