DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT register_table_export('distributors', '{did,name}', 4);

-- To Launch ingestor background worker
SELECT ingestor_launch();