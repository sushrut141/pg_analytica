DROP TABLE test_data;

CREATE TABLE test_data (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  age INTEGER NOT NULL,
  is_random boolean,
  rating float4,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ~5 GiB table
INSERT INTO test_data (name, age, is_random, rating)
SELECT
  floor(random() * 1000) || ' Name',  -- Generate random names
  floor(random() * 100) + 18 AS age,  -- Generate random ages between 18-117
  random() < 0.5 as is_random,
  random() as rating
FROM generate_series(1, 10000000);