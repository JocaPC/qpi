# Baseline

Query Performance Insight library enables you to get the baseline of your workload.

First, you should get the snapshot of our performance counters using `qpi.snapshot_perf_counters` procedure:
```
EXEC qpi.snapshot_perf_counters;
```
Then you can get the distribution of the query executions:
```
SELECT
     bcount.name,
	 duration_avg_ms = CASE WHEN bcount.value = 0 THEN 0 ELSE btime.value/bcount.value END , 
     exec_count = CAST(bcount.value AS BIGINT),
	 exec_count_perc = CAST((100.0 * bcount.value/SUM (bcount.value) OVER()) AS DECIMAL(5,2)),
	 exec_time_perc = CAST((100.0 * btime.value/SUM (btime.value) OVER()) AS DECIMAL(5,2)) 
FROM
(
     SELECT *
     FROM qpi.perf_counters
     WHERE instance_name = 'Elapsed Time:Requests'
) bcount
JOIN
(
     SELECT *
     FROM qpi.perf_counters
     WHERE instance_name = 'Elapsed Time:Total(ms)'
) btime ON bcount.name = btime.name
ORDER BY bcount.name ASC 
```

For more details see [Creating quick performance baseline](https://blogs.msdn.microsoft.com/sql_pfe_blog/2016/10/11/create-a-quick-and-easy-performance-baseline/).