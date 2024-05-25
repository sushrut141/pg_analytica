DROP TABLE test_data;

CREATE TABLE test_data (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  age INTEGER NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ~2 GiB table
INSERT INTO test_data (name, age)
SELECT
  floor(random() * 1000) || ' Name',  -- Generate random names
  floor(random() * 100) + 18 AS age  -- Generate random ages between 18-117
FROM generate_series(1, 30000000);