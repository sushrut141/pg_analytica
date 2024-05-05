# Notes on pg_analytica extension

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

