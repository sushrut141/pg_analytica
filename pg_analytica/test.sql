DROP EXTENSION IF EXISTS pg_analytica CASCADE;
CREATE EXTENSION pg_analytica;


CREATE TABLE columnar_distributors
USING pg_analytica_am
AS SELECT * FROM distributors;
