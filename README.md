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
You can stop PostgreSQL using the brew services stop postgresql command.
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
Create a new directory and place four files there named get_sum.c, get_sum.control, Makefile and get_sum--0.0.1.sql.

Run make command in the above directory:  

```python
make 
make install
```
Start the postgreSQL:

```python
psql postgres
```
Install the extension in postgres with CREATE EXTENSION get_sum and check the utility with SELECT get_sum(m,n) command:

```python
postgres=# CREATE EXTENSION get_sum;
CREATE EXTENSION
postgres=# SELECT get_sum(2,3);
 get_sum 
---------
       5
(1 row)
```

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://gitlab.com/shenterprises047/pg_analytica.git
git branch -M main
git push -uf origin main
```

## License
For open source projects, say how it is licensed.

