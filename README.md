# pg_analytica

## Postgres Installation using Homebrew:

```
brew install postgresql
```
After you install PostgreSQL on your Mac, you will be able to start it via Terminal using the following command:
```
brew services start postgresql
```
```
Note:
You can stop PostgreSQL using the `brew services stop postgresql` command.
```
Enter into Postgres using below command:

```
psql postgres
```
## Check PostgreSQL version and PATH

Check the version and PATH with below command: 

```
pg_config --pkglibdir         
```
The output should be:
/opt/homebrew/lib/postgresql@14


## Running a sample extension

Create a new directory and place four files there named pg_analytica.c, pg_analytica.control, Makefile and pg_analytica--0.0.1.sql.

Run make command in the above directory:  

```python
make CC='/usr/bin/gcc'
make install
```

## Generate sample data for testing

Run the below command.

```
psql postgres -f test.sql
```

This creates a table and populates sample rows that can used in testing.

## Testing the extension

Add tests to the test.sql file.

Tests can be executed using the command below.
```
psql postgres -f test.sql
```

## Viewing Logs

Logs can be viewed at.

```
tail -100 /opt/homebrew/var/log/postgresql@14.log
```

