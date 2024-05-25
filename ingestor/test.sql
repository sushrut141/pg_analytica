DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT register_table_export('test_data', '{id,name,age}', 7);

-- To Launch ingestor background worker
SELECT ingestor_launch();