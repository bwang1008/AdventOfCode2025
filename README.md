# Advent of Code 2025 Solutions

Using Ubuntu 20.04 on WSL.

Installed Postgresql with

```sh
sudo apt install postgresql
```
which installed version 12.22.

Started service with

```sh
sudo service postgresql start
```
based off of https://stackoverflow.com/questions/30095546/postgresql-error-could-not-connect-to-database-template1-could-not-connect-to


Change to `postgres` user:
```sh
sudo -u postgres -i
psql
```
then create a database and my own role:

```sql
CREATE DATABASE advent2025;
CREATE USER bwang1008 WITH PASSWORD '*****';
GRANT ALL PRIVILEGES ON DATABASE advent2025 TO bwang1008;
```

Exiting the `psql` session and the `postgres` user, access the local Postgres server with

```sh
psql --host=localhost --port=5432 --username="bwang1008" --dbname=advent2025
```

Run a Postgres script like the following:

```sh
psql --host=localhost --port=5432 --username="bwang1008" --dbname=advent2025 -f day01/day01A.sql
```

with the following sample output:
```sql
COPY 4753
 count
-------
  1191
(1 row)
```
where 1191 is the final answer.
