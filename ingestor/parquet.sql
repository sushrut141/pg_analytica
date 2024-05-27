-- function to list all parquet iles in data directory for table
create function list_parquet_files(args jsonb)
returns text[] as
$$
begin
    return array_agg(args->>'dir' || '/' || filename)
           from pg_ls_dir(args->>'dir') as files(filename)
           where filename ~~ '%.parquet';
end
$$
language plpgsql;

-- Register parquet files as foreign table
create foreign table test_data_columnar (
    id           int,
    name   varchar(40),
    age          int
)
server parquet_srv
options (
    files_func 'list_parquet_files',
    files_func_arg '{"dir": "./pg_analytica/test_data"}',
    use_mmap 'true',
    use_threads 'true'
);
