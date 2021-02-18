# db-insights
DB Insights is a tool kit for validating data. It contains premade checks for important data checks. DB Insights provides out-of-the box checks which emit statsd metrics and logs -- alerting you to the health of your database, and the integrity of your data.

DB insights is a single deployable binary that you configure to point at 1 or more databases. Currently it supports the following checks for the following databases:


|           | Snowflake          | Redshift | Postgres |
|-----------|--------------------|----------| ---- |
| Freshness | :white_check_mark: | :x:      | :white_check_mark: |
| Comparisons | :white_check_mark: - Primary | :white_check_mark: - Secondary     | :x: |
|           |                    |          | |


# Checks

## Freshness

Freshness checks the time of the most recent record within a table.

### Conf

```
# examples/freshness.yml

freshness:
  targets:
    - database: test
      schema: public
      table: test
      column: created_at
```

### Execution

- Start postgres locally using docker
```
$ docker-compose up -d
```

- Verify that the tset table was created
```
$ psql -h localhost -U test
Password for user test:
psql (12.4)
Type "help" for help.

test=#
test=# \dt
       List of relations
 Schema | Name | Type  | Owner
--------+------+-------+-------
 public | test | table | test
(1 row)

test=# select * from test;
         created_at
----------------------------
 2021-02-18 19:32:00.519778
(1 row)
```

- export your connection string
```
$ export DB_INSIGHTS_CONN_STRING="dbname=test user=test password=test host=localhost sslmode=disable"
```

- invoke the check
```
$ go run cmd/dbinsights/main.go -conf=examples/freshness.yml -db=postgres check freshness
hi
DEBU[0000] Freshness Conf: {Targets:[{Database:test Schema:public Table:test Column:created_at}]}
DEBU[0000] postgres.Freshness: executed: "SELECT max(created_at) FROM test.public.test"
DEBU[0000] EmitAll: [{test public test 2021-02-18 19:51:11.490488 +0000 +0000 created_at}]
DEBU[0000] freshness table: test.public.test, total_seconds: 2m43.758708s
```


## Comparison 

Comparisons query 2 different databases and emits metrics about the results of the query. Comparison queries must return

(string, int). All strings must be the same keys.

### Conf


### Execution
