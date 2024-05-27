DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT register_table_export('distributors', '{did,name}', 7, 3);

-- To Launch ingestor background worker
SELECT ingestor_launch();