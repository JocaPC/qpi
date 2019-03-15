# Analyzing performance

Query Performance Insight library enables you to easily get the performance insights in your SQL Server Database Engine. Here are shown some query examples. 

The following query returns number of requests per second and CPU% used by queries per query store intervals:
```
select start_time, execution_type_desc,
	tps =  sum(count_executions)/ min(interval_mi) /60,
	[cpu %] = ROUND(100 * sum(count_executions*cpu_time_s)/ min(interval_mi) /60 /(SELECT top 1 cpu_count FROM sys.os_sys_info)/*cores*/,1)
from qpi.db_query_plan_exec_stats_history
group by start_time, execution_type_desc
order by start_time desc
```

The following query enables you to create file snapshots and analyze IO statistics:
```
EXEC qpi.snapshot_file_stats @title = 'start';
--> run some query or workload
EXEC qpi.snapshot_file_stats @title = 'M1';
--> run some query or workload
EXEC qpi.snapshot_file_stats @title = 'M2';
--> run some query or workload
EXEC qpi.snapshot_file_stats @title = 'M2';
--> run some query or workload
EXEC qpi.snapshot_file_stats @title = 'M3';

SELECT *
FROM qpi.file_stats_as_of( qpi.ago(0,2,15) );


SELECT *
FROM qpi.file_stats_at( 'M2' )

SELECT *
FROM qpi.file_stats
WHERE database_name = 'tpcc5000'
```

You can also view wait statistics on the instance:
```
select category, wait_s = sum(wait_time_ms)/1000
from qpi.wait_stats
group by category_id, category
order by sum(wait_time_ms) desc
```