--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION get_sum" to load this file. \quit

CREATE OR REPLACE FUNCTION get_sum(int, int) RETURNS int
AS '$libdir/get_sum'
LANGUAGE C IMMUTABLE STRICT;
