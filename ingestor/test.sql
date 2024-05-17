DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

-- To Launch ingestor background worker
SELECT ingestor_launch('distributors', '{"did"}');

-- To register table for export
SELECT register_export();