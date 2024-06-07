## Rough Notes on pg_analytica extension


## Approach 1

Try creating a columnar table by running a `CREATE TABLE AS` query by reading data from
an existing table using a custom table access method to write the data.

To keep the table updated as new rows are added, we would need to explore other techniques.
May create a hook that periodically writes new tuples from source table. 
Maybe we can keep track of the oldest txn id ingested and query rows with txn id greater than that and write to columnar store.

 - pg_analytica extension defines a custom table access method
    - When used the following methods are called
        - `memam_relation_set_new_filelocator`
        - `memam_relation_needs_toast_table`
        - `memam_tuple_insert` called for each row in source table
            - Data in tuples is empty, number of valid attributes `tts_nvalid` is 0.
            - Calling `ExecFetchSlotHeapTuple` fetches tuple with actual data in it.

     - `memam_relation_set_new_filelocator` called to create storage for new table
        - `RelationCreateStorage` can be called to create the create the storage manager relation
           `SMgrRelation` used by postgres to access the table storage
        - Citus maps columnar storage concepts to postgres concepts like pages
        - `CatalogTupleInsert` is called at the end to insert table information into postgres catalog

## Approach 2
     - Table Access Manager definition only support row by row insert for `CREATE TABLE AS` queries
        - Multi-insert is only called if data is dumped to disk and read as a file
        - `CTAS` appoach to create columnar store may not work
     
     - Creating columnar tables in background process
        - Extension creates a metadata table to store metadata about columnar tables
        - Calling a method exposed by extension adds table entry to metadata table
            - A new table is also created in catalog that can use custom table AM handler ??
                - Can we override query behavior here?
        - Background worker periodically scans registered tables and reads a subset of rows and writes it as a columnar file
            - After writing, the latest transaction id is written to metadata table
                - instead of transaction id we can read using OFFSET and chunk as well
                    ```
                    SELECT * FROM your_table
                    OFFSET offset
                    LIMIT chunk_size;
                    ```
                 - For determinitoc order we can ask users to supply list of columns by which
                   rows should be ordered. Columnar files will be sorted by these columns.
            - In subsequent runs, only rows after latest transaction id / offset are scanned
            - Alternatively, we can scan the entire table each time.
        - Another background worker periodically combines multiple columnar files for the same table into a single file
        - Multiple ways of initializing a file defined here
          `postgres/src/include/storage/fd.h`
        - Initializing a subdirectory in postgres data directory here
          `MakePGDirectory(const char *directoryName)`
        - Details about creating background worker here
          `postgres/src/include/postmaster/bgworker.h`
        - Example usage of background worker
          `postgres/src/test/modules/worker_spi/worker_spi.c`

## Reading columnar file created by ingestor

The columnar file is created in the postgres data directory (PGDATA).
PGDATA directory can be found using this sql query.
```
SELECT current_setting('data_directory') AS pgdata_path;
```

To read the parquet file, start a duck db instance in memory and
execute the select query to read the parquet file.

```
$ duckdb

SELECT * from read_parquet('/opt/homebrew/var/postgresql@14/sample.parquet');
```

# Query using Parquet Foreign Data Wrapper
 - https://github.com/adjust/parquet_fdw/blob/master/README.md

# Query timings


### Test 1

Measured using the timing feature in postgres.
 - Table Size little over 2GiB
    - 30M rows with 2 int64 columns and a varchar column exported

```
\timing on
```

 - select with group by and filter
```
SELECT 
    age,
    COUNT(*) 
FROM test_data_columnar
WHERE age > 50 AND age < 90 AND id > 1000
GROUP BY age;

Time: 1633.900 ms (00:01.634)

SELECT 
    age,
    COUNT(*) 
FROM test_data
WHERE age > 50 AND age < 90 AND id > 1000
GROUP BY age;

Time: 5153.393 ms (00:05.153)
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
postgres=# SELECT COUNT(*) from analytica_test_data WHERE age > 50 AND age < 55;
  count  
---------
 2398998
(1 row)

Time: 1399.673 ms (00:01.400)


postgres=# SELECT COUNT(*) from test_data WHERE age > 50 AND age < 55;
  count  
---------
 2399709
(1 row)

Time: 9904.160 ms (00:09.904)

```

## Export timings

### Test 1

Time to export can be reduced with variable exort chunk sizes.
Users can choose larger chunk size depending on size of row and system memory.

  - Table Size little over 2GiB
      - 30M rows with 2 int64 columns and a varchar column exported
      - export chunk size 10k
      - start time 12:21 PM
      - 10M / 30M rows processed at 12:33 PM
      - Finished processing at 13:34 PM
      - Overal export time - 1hr 13m
      - Max Memory usage - 3.2GiB
      - 3k columnar files exported

### Test 2

- Table Size little over 5GiB
  - export chunk size 100k
  - Started at 8:47 PM
  - Peak memory usage steadily growing, currently at 9GiB
    - possible memory leak
  - memory usage at 9.82GiB
  - Completed at 9:34 PM
  - Peak memory usage 10.35GiB
  - 40min to export 5GiB of data
  - 600 columnar files exported

## Tasks

 - Look into memory usage increase
  - see if using cursors to read chunks reduces memory usage
  - check for memory leaks
 - Create section in readme about perf improvements

 - Add export timing metrics to metadata
 - Support exporting columnar files sorted by keys