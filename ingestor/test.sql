DROP EXTENSION ingestor;
CREATE EXTENSION ingestor;

SELECT register_table_export(
    -- table name
    'test_data', 
    -- columns to export
    '{id,name,age,is_random,rating,created_at}',
    -- export_frequency_hours
    7
);

-- To Launch ingestor background worker
SELECT ingestor_launch();