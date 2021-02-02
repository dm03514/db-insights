# db-insights
DBInsights is a tool kit for validating data. It contains premade checks for important data invariants. DBInsights provides out-of-the box checks which emit statsd metrics and logs, alerting you to the health of your database and the integrity of your data.

DB insights is a single deployable binary that you configure to point at 1 or more databases. Currently it supports:

- Last insert / update times on tables
- Partition Integrity 
