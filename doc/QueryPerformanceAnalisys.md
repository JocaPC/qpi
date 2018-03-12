# Analyzing performance

Query Performance Insight library enables you to easily get the performance insights in your SQL Server Database Engine. Here are shown some query examples. 

The following query returns number of requests per second and CPU% used by queries per query store intervals:
```
select start_time,
	tps =  sum(count_executions)/ min(interval_mi) /60,
	tph =  sum(count_executions) * 60 / min(interval_mi),
	[cpu %] = ROUND(100 * sum(count_executions*cpu_time_s)/ min(interval_mi) /60 /(SELECT top 1 cpu_count FROM sys.dm_os_sys_info)/*cores*/,1)
from qpi.query_stats
where interval_mi is not null
and execution_type_desc = 'Regular'
group by start_time
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
FROM qpi.runtime_file_stats_on( qpi.ago(215) );


SELECT *
FROM qpi.runtime_file_stats_at( 'M2' )

SELECT *
FROM qpi.runtime_file_stats
WHERE database_name = 'tpcc5000'
```
