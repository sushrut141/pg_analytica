# Notes on pg_analytica extension

## Approach 1

Try creating a columnar table by running a `CREATE TABLE AS` query by reading data from
an existing table using a custom table access method to write the data.

To keep the table updated as new rows are added, we would need to explore other techniques.
May create a hook that periodically writes new tuples from source table. 
Maybe we can keep track of the oldest txn id ingested and query rows with txn id greater than that and write to columnar store.

 - pg_analytica extension defines a custom table access method
    - When used the following methods are called
        - memam_relation_set_new_filelocator
        - memam_relation_needs_toast_table
        - memam_tuple_insert called for each row in source table