![pg_analytica Banner](images/pg_analytica.jpg)

## What is pg_analytica

*pg_analytica* is a Postgres extension that speeds up analytics queries by upto 90%. 

### How it works

*pg_analytica* automatically exports specified columns from your table into [columnar format](https://parquet.apache.org) at regular intervals. The columnar storage allows for blazing-fast querying compared to traditional row-based storage.

### Getting Started

### Download extension dependencies

The extension relies on the following dependencies.
 - parquet-glib
 - arrow-glib
 - gio-2.0

Installation instructions for various platforms can be found on the [Apache Arrow docs](https://arrow.apache.org/install/).

The extension relies on the [parquet_fdw](https://github.com/adjust/parquet_fdw) extension to query the exported columnar data. Follow the [installation](https://github.com/adjust/parquet_fdw?tab=readme-ov-file#installation) instructions on the repo.

Ensure that the extension is loaded before proceeding with next steps.
Running the query below within a psql session should show that the extension is installed.

```
postgres=# SELECT * FROM pg_extension WHERE extname = 'parquet_fdw';
```


### Installing pg_analytica

Clone the extension to your workstation and navigate to the ingestor directory.

```
$ cd ingestor
```

Build and install the extension to your Postgres install path.

```
$ make
$ make install
```

### Load the extension

Start a psql session and load the extension.
```
$ psql postgres
```

Within the session, load the extension.
```
postgres=# CREATE EXTENSION ingestor;
```

### Register a table for export

The extension periodically exports data from registered tables in columnar format to
provide speed ups in querying. To register a table for export use the command below
in the psql session where the extension was loaded.

```
postgres=# SELECT register_table_export(
    -- table name
    'your_table_name', 
    -- list of columns to export
    '{column_1,column_2,column_3}',
    -- Hours after which table data will be re-exported
    10
);
```

*pg_analytica* supports the following column types currently.

| PG Type        | Support               |
| -------------- | --------------------- |
| smallint       | :white_check_mark     |
| integer        | :white_check_mark     |
| bigint         | :white_check_mark     |
| float          | :white_check_mark     |
| double         | :white_check_mark     |
| boolean        | :white_check_mark     |
| varchar        | :white_check_mark     |
| text           | :white_check_mark     |
| boolean        | :white_check_mark     |
| timestamp      | :white_check_mark     |

### Start the export background worker

The ingestion background worker periodically finds registered tables eligible
for export and exports the data. The worker only needs to be launched once.

```
postgres=# SELECT ingestor_launch();
```

Once the export process is complete, you will be able to query the table.
To check if your table is ready for export see if your table is listed in the result
for the following query.

```
postgres=# SELECT * FROM analytica_exports WHERE last_run_completed IS NOT NULL;
```

During testing exporting a table with 60M rows and 5 columns took 40min but numbers may vary depending on your machine and size of the table.

### Querying the exported table

Exported tables are queryable using the relation name `analytica_{table_name}`.
If the table name you registered was `test_data` then the exported table will be queried as follows.
```
postgres=# SELECT * FROM analytica_test_data;
```

### Benchmarks

The extension was tested on a Postgres instance running on an M1 Air Macbook. To see the table used for testing see [here](./ingestor/generate_test_data.sql).
Speed ups in querying columnar store vary based on the query but tests indicate upto 90% decease in query latency!


### Test 1

Measured using the timing feature in postgres.
 - Table Size little over 2GiB
 - 30M rows with 2 int64 columns and a varchar column exported

```
\timing on


-- Query on row based data
postgres=# SELECT 
    age,
    COUNT(*) 
FROM test_data
WHERE age > 50 AND age < 90 AND id > 1000
GROUP BY age;

Time: 5153.393 ms (00:05.153)


-- Query on columnar data
postgres=# SELECT 
    age,
    COUNT(*) 
FROM analytica_test_data
WHERE age > 50 AND age < 90 AND id > 1000
GROUP BY age;

Time: 1633.900 ms (00:01.634)
```

### Test 2

Measured using the timing feature in postgres.
 - Table Size little over 5GiB
 - 60M rows with 6 columns of different types
 
```
\timing on


-- Query on row based data
SELECT 
    age,
    COUNT(*) 
FROM test_data
WHERE age > 50 AND age < 90 AND is_random
GROUP BY age;

Time: 9446.380 ms (00:09.446)


-- Query on columnar data
SELECT 
    age,
    COUNT(*) 
FROM analytica_test_data
WHERE age > 50 AND age < 90 AND is_random
GROUP BY age;

Time: 2312.845 ms (00:02.313)

postgres=# \timing on
Timing is on.

-- Query on row based data
postgres=# SELECT COUNT(*) from test_data WHERE age > 50 AND age < 55;
  count  
---------
 2399709
(1 row)

Time: 9904.160 ms (00:09.904)


-- Query on columnar data
postgres=# SELECT COUNT(*) from analytica_test_data WHERE age > 50 AND age < 55;
  count  
---------
 2398998
(1 row)

Time: 1399.673 ms (00:01.400)
```