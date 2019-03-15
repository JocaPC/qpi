# Analyze Query performance
Query Performance Insight library enables you to analyze query and plan performance information.

## Query runtime performance
QPI library enables you to easily get information about the query plan performance using `qpi.db_query_exec_stats` view.
```
SELECT *,
		log_throughput_kbps = log_bytes_used_kb /interval_mi /60
FROM qpi.db_query_exec_stats
```

In order to get the query performance information on some specific time interval, you can use `qpi.db_query_exec_stats_as_of` function:
```
SELECT * 
FROM qpi.db_query_exec_stats_as_of( '2018-02-25T16:30:00.0000' );
```

This query will return the same information as the previous one, but in the time window for the date provided as the argument. Time window is the Query Store interval length for this period.

> `qpi.ago()` is a helper function that returns a datetime in past. `qpi.ago(1,3,15)` will return the date 1 day, 3 hours, and 15 minutes ago. Instead of calculating time or using `DATEDIFF` function, you can use this function to go back in time. The following example goes two and half hours back in time:
```
SELECT * 
FROM qpi.db_query_exec_stats_as_of(qpi.ago(0,2,30));
```

If you are interested in some particular query plan, you can use view `qpi.db_query_plan_exec_stats_as_of` and filter results by `plan_id`:
```
SELECT * 
FROM qpi.db_query_plan_exec_stats_as_of(qpi.ago(1,0,0))
WHERE plan_id = 1815;
```

## Comparing performance

QPI library enables you to easily compare runtime performance of the queries in some period of time.
You just need to specify in what time intervals you want to see the differences:

```
SELECT *
FROM qpi.db_query_plan_exec_stats_diff (qpi.ago(0,2,0), GETDATE());
```
The query will return absolute and percentage differences of the average query performance statistics (avg. CPU, avg. duration, etc.)
If you want to see performance differences for some particular query or plan, you can filter data by `query_id` or `plan_id`:
```
SELECT *
FROM qpi.db_query_plan_exec_stats_diff (qpi.ago(0,2,0), GETDATE())
WHERE query_id = 1389;

select *
from qpi.db_query_plan_exec_stats_diff (qpi.ago(0,2,0), GETDATE())
WHERE plan_id = 1804;
```

> Use this function to identify are the performance improvements or degradations after you make some optimization in your database. Usually, you will identify a query that is running slow, run a workload, create some index that might help, and then execute this query to identify are the performance improved compared to the previous interval.
