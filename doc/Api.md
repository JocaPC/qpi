# Query Performance Insights API

Query performance insights (QPI) library provides various views and functions that can help you to get more information about the performace of your system. In this page your can see the overview of QPI API.

## System information

QPI library enables you to easily find basic system information that describe your SQL Server instance. This library contains views that you can use to find number of CPU, size of memory, information about files, using the following views: 
 - `qpi.sys_info` returns system information (number of cores, available memory)
 - `qpi.cpu_usage` - returns information about the CPU usage.
 - `qpi.mem_usage` - returns information about the memory usage.
 - `qpi.db_mem_usage` - returns information about the memory usage in database.
 - `qpi.mem_plan_cache_info` - returns information about the memory usage in plan cache.

Find more information in [system information page](doc/SystemInfo.md).

## Queries

QPI library enables you to find information about the queries that are executed on your SQL Server Database Engine using the following views/functions:
 - `qpi.db_queries` - returns the queries that are executed in SQL Server Database Engine.
 - `qpi.db_queries_ex` - returns the queries that are executed in SQL Server Database Engine including information about context parameters.
 - `qpi.queries` - returns the currently executing queries.
 - `qpi.db_query_texts` - returns the query texts that are executed in SQL Server Database Engine.
 - `qpi.blocked_queries` - returns the currently blocked queries.
 - `qpi.query_locks` - returns the locks obtained by queries.

 Find more information in [query information page](doc/QueryInfo.md).

## Query Statistics

QPI library enables you to find statistics that can tell you more about query performance using the following views/functions:

 - `qpi.db_query_stats` - returns summarized executtion and wait statistics about the queries.
 - `qpi.db_query_exec_stats` - returns execution statistics about the queries.
 - `qpi.db_query_exec_stats_history`
 - `qpi.db_query_exec_stats_as_of`
 - `qpi.db_query_plan_exec_stats` - returns execution statistics about the query plans.
 - `qpi.db_query_plan_exec_stats_ex` - returns execution statistics about the query plans including the plan SET options.
 - `qpi.db_query_plan_exec_stats_history` - returns all known execution statistics about any query plans in the past.
 - `qpi.db_query_plan_exec_stats_as_of ` - returns execution statistics about the query plans at the specified point in time in the past.
 - `qpi.db_query_stats` - returns execution stats about the currently executing queries.
 - `qpi.wait_stats` - returns information about wait statistics.
 - `qpi.wait_stats_as_of` - returns information about wait statistics at the specified point in time in the past.
 - `qpi.snapshot_wait_stats` - the procedure that takes a snapshot of [sys.dm_os_wait_stats](https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql?view=sql-server-2017) and clears the wait stats.
 - `qpi.db_query_wait_stats` - returns the wait statistics about the queries from Query Store.
 - `qpi.db_query_wait_stats_as_of` - returns the wait statistics about the queries from Query Store at the specified point in time in the past.
 - `qpi.db_query_plan_wait_stats` - returns the wait statistics about the query plans from Query Store.
 - `qpi.db_query_plan_wait_stats_as_of` - returns the wait statistics about the query plans from Query Store at the specified point in time in the past.
 
Find more information in [query performance page](doc/QueryStatistics.md).

## File performance

QPI library enables you to find performance of underlying file system using the following view/procedure:
 - `qpi.file_stats` - returns information about file perfromance.
 - `qpi.snapshot_file_stats` - the procedure that takes a snapshot of [sys.dm_os_virtual_file_stats](https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-io-virtual-file-stats-transact-sql?view=sql-server-2017) and calculated latentcies and throughput.

Find more information in [file statistics page](doc/FileStatistics.md).

## Performance counters

QPI library enables you to get the perfromance counter values that show the state of your system:
 - `qpi.perf_counters` is a view that returns information about various performance counters. Some values are not available until you take a snapshot of performance counters.
 - `qpi.snapshot_perf_counters` that takes a snapshot of the perf counter values needed to calculate some perf counters that depend on previous values.