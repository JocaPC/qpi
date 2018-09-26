# File performance

One of the important tasks that you might need to do is to find the file performance statistics (IOPS, throughput, etc.) SQL Server Database Engine provides cumulative file performance measurements in `sys.dm_io_virtual_file_stats ()` function. However, in order to get the runtime file performance statistics, you would need to take the snapshots of this function and analyze differences between two snapshots.

Query Performance Insight library enables you to monitor statistics that can help you identify
file performance statistics using `qpi.file_stats` view. First you need to take a snapshot of the current file statistics using `qpi.snapshot_file_stats` procedure and then get the file performance statistics when the workload finishes:

```
EXEC qpi.snapshot_file_stats;

--> run some query or workload

SELECT *
FROM qpi.file_stats;
```

> If you stop the workload and call `qpi.file_stats` later, you will see that some values are  different (for example, throughput and IOPS are decreased). The reason is the fact that `qpi.file_stats` view divides performance counter values with the elapsed time from the snapshot. 
In order to get consistent results, you can take multiple snapshots of the file performance counters:

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
```

When you take the snapshots of the file statistics, you can find information about the file performance at some point of time:
```
SELECT *
FROM qpi.file_stats_as_of( qpi.ago(0, 2, 15) );
```

Since the snapshots are labeled, you can get file performance at some snapshot point:
```
SELECT *
FROM qpi.file_stats_at('M2');
```

You can see all available snapshot names using the `qpi.file_stats_snapshots` view.

```
SELECT *
FROM qpi.file_stats_snapshots
ORDER BY start_time
```

If you want to see file statistics for the current database you can execute the following query:

```
select	file_name, size_gb, 
		throughput_mbps, read_mbps, write_mbps,
		iops, write_iops, read_iops,
		latency_ms, write_latency_ms, read_latency_ms,
		kb_per_read, kb_per_write
from qpi.db_file_stats
```



In the `snapshot_name` column you will see the titles that you can use in `qpi.file_stats_at` function.

> You can create a SQL Agent job that periodically takes the snapshots of file performance counters.

