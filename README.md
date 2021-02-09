# db-insights
DB Insights is a tool kit for validating data. It contains premade checks for important data checks. DB Insights provides out-of-the box checks which emit statsd metrics and logs -- alerting you to the health of your database, and the integrity of your data.

DB insights is a single deployable binary that you configure to point at 1 or more databases. Currently it supports the following checks for the following databases:


|           | Snowflake          | Redshift |
|-----------|--------------------|----------|
| Freshness | :white_check_mark: | :x:      |
|           |                    |          |


# Checks

## Freshness

Freshness checks the time of the most recent record within a table.

### Conf

```
# path/to/your/config.yml

freshness:
  targets:
    - database: yourdb 
      schema: yourschema 
      table: yourtable 
      column: yourcolumn 

```

### Execution

```
# export your connection string
export DB_INSIGHTS_CONN_STRING="user:pw@ACCOUNT.REGION/DB/?role=ROLE&warehouse=WAREHOUSE"

# invoke the check
$ go run cmd/dbinsigts/main.go --conf=path/to/your/config.yml check freshness
```
