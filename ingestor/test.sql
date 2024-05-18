DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT register_table_export('distributors', '{"column1","column2"}', 4);

-- To Launch ingestor background worker
SELECT ingestor_launch('distributors', '{"did"}');

-- To register table for export
SELECT register_export();