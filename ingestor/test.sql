DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT ingestor_launch('distributors', '{"did"}');