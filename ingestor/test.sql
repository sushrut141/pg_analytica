DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT register_table_export('distributors', '{did,name,description,alpha_level,created,hokage}', 7, 3);

-- To Launch ingestor background worker
SELECT ingestor_launch();