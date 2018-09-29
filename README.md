# Query Performance Insights

Query Performance Insights (QPI) is a collection of useful scripts that enable you find what is happening with your SQL Server. It is a set of views and functions that wrap Query Store and Dynamic Management Views.

## Why I need this kind of library?

SQL Server/Azure SQL Database provide a lot of views that we can use to analyze query performance (dynamic management views and Query Store views). However, sometime it is hard to see what is happening in the database engine. There are DMVs and Query Store, but when we need to get the answer to the simple questions such as "Did query performance changed after I added an index?" or "How many IOPS do we use?", we need to dig into Query Store schema, think about every single report, or search for some useful query.

This is the reason why I have collected the most useful queries that find information from underlying system views, and wrapped them in a set of useful views. Some usefull resources that I have used:

 - Paul Randal [Wait statistics library](https://www.sqlskills.com/help/waits/), [Wait statistics - tell me where it hurts](https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/), [How to examine IO subsystme latencies](https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/)
 - Erin Stellato [What Virtual Filestats Do, and Do Not, Tell You About I/O Latency](https://sqlperformance.com/2013/10/t-sql-queries/io-latency).
 - Aaron Bertrand [Determine system memory](https://www.mssqltips.com/sqlservertip/2393/determine-sql-server-memory-use-by-database-and-object/)
 - Dimitri Furman & [SqlCat team](https://blogs.msdn.microsoft.com/sqlcat/) blog posts
 - Tim Ford & Louis Davidson [Performance tunning with DMV](https://www.red-gate.com/library/performance-tuning-with-sql-server-dynamic-management-views), 

## System information

QPI library enables you to easily find basic system information that describe your SQL Server instance. This library contains views that you can use to find number of CPU, size of memory, information about files, using the following views: 
 - `qpi.sys_info` returns system information (number of cores, available memory)
 - `qpi.dm_cpu_usage` - returns information about the CPU usage.
 - `qpi.dm_mem_usage` - returns information about the memory usage.
 - `qpi.dm_db_mem_usage` - returns information about the memory usage in database.
 - `qpi.dm_mem_plan_cache_info` - returns information about the memory usage in plan cache.

Find more information in [system information page](doc/SystemInfo.md).

## Queries

QPI library enables you to find information about the queries that are executed on your SQL Server Database Engine using the following views/functions:
 - `qpi.queries` - returns the queries that are executed in SQL Server Database Engine.
 - `qpi.queries_ex` - returns the queries that are executed in SQL Server Database Engine including information about context parameters.
 - `qpi.dm_queries` - returns the currently executing queries.
 - `qpi.query_texts` - returns the query texts that are executed in SQL Server Database Engine.
 - `qpi.dm_blocked_queries` - returns the currently blocked queries.
 - `qpi.dm_query_locks` - returns the locks obtained by queries.

 Find more information in [query information page](doc/QueryInfo.md).

## Query Statistics

QPI library enables you to find statistics that can tell you more about query performance using the following views/functions:
 - `qpi.query_stats` - returns execution statistics about the queries.
 - `qpi.query_stats_all`
 - `qpi.query_stats_as_of`
 - `qpi.query_plan_stats` - returns execution statistics about the query plans.
 - `qpi.query_plan_stats_ex` - returns execution statistics about the query plans including the plan SET options.
 - `qpi.query_plan_stats_all` - returns all known execution statistics about any query plans in the past.
 - `qpi.query_plan_stats_as_of ` - returns execution statistics about the query plans at the specified point in time in the past.
 - `qpi.dm_query_stats` - returns execution stats about the currently executing queries.
 - `qpi.wait_stats` - returns information about wait statistics.
 - `qpi.wait_stats_as_of` - returns information about wait statistics at the specified point in time in the past.
 - `qpi.snapshot_wait_stats` - the procedure that takes a snapshot of [sys.dm_os_wait_stats](https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql?view=sql-server-2017) and clears the wait stats.
 - `qpi.query_wait_stats` - returns the wait statistics about the queries from Query Store.
 - `qpi.query_wait_stats_as_of` - returns the wait statistics about the queries from Query Store at the specified point in time in the past.
 - `qpi.query_plan_wait_stats` - returns the wait statistics about the query plans from Query Store.
 - `qpi.query_plan_wait_stats_as_of` - returns the wait statistics about the query plans from Query Store at the specified point in time in the past.
 
Find more information in [query performance page](doc/QueryStatistics.md).

## File performance

QPI library enables you to find performance of underlying file system using the following view/procedure:
 - `qpi.file_stats` - returns information about file perfromance.
 - `qpi.snapshot_file_stats` - the procedure that takes a snapshot of [sys.dm_os_virtual_file_stats](https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-io-virtual-file-stats-transact-sql?view=sql-server-2017) and calculated latentcies and throughput.

Find more information in [file statistics page](doc/FileStatistics.md).

## Performance analysis

QPI library simplifies query performance analysis. Some useful scripts and scenarios are shown in [query performance analysis page](doc/QueryPerformanceAnalisys.md).

## Installation
QPI library is just a set of views functions and utility tables that you can install on your SQL Server or Azure SQL instance. Currently, it supports SQL Server 2016+ and Azure SQL Database.
You can download the [source](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.sql) and run it in your database.

> Many views depends on Query Store so make sure that Query store is running on your SQL Server.