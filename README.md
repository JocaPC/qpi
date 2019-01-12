# Query Performance Insights

Query Performance Insights (QPI) is a collection of useful scripts that enable you find what is happening with your Azure SQL Database Managed Instance (most of the scripts would work on SQL Server 2016+). It is a set of views and functions that wrap Query Store and Dynamic Management Views. See how to install the proper version for your SQL Server in [Installation](#installation) section.

## Why I need this kind of library?

SQL Server/Azure SQL Database provide a lot of views that we can use to analyze query performance (dynamic management views and Query Store views). However, sometime it is hard to see what is happening in the database engine. There are DMVs and Query Store, but when we need to get the answer to the simple questions such as "Did query performance changed after I added an index?" or "How many IOPS do we use?", we need to dig into Query Store schema, think about every single report, or search for some useful query. Also, for most of the views, you would need to read several articles to understand how to interpret the results.

This is the reason why I have collected the most useful queries that find information from underlying system views, and wrapped them in a set of useful views. Some usefull resources that I have used:

 - Paul Randal [Wait statistics library](https://www.sqlskills.com/help/waits/), [Wait statistics - tell me where it hurts](https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/), [How to examine IO subsystem latencies](https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/)
 - Erin Stellato [What Virtual Filestats Do, and Do Not, Tell You About I/O Latency](https://sqlperformance.com/2013/10/t-sql-queries/io-latency).
 - Aaron Bertrand [Determine system memory](https://www.mssqltips.com/sqlservertip/2393/determine-sql-server-memory-use-by-database-and-object/)
 - Dimitri Furman & [SqlCat team](https://blogs.msdn.microsoft.com/sqlcat/) blog posts
 - Tim Ford & Louis Davidson [Performance tunning with DMV](https://www.red-gate.com/library/performance-tuning-with-sql-server-dynamic-management-views), 
 - Ajith Krishnan [Interpreting the counter values from sys.dm_os_performance_counters](https://blogs.msdn.microsoft.com/psssql/2013/09/23/interpreting-the-counter-values-from-sys-dm_os_performance_counters/).

## Examples

Let's start with some common examples of the queries that I use.

Getting information abouth your workload:
```
select * from qpi.queries; -- All recorded queries in Query Store

select * from qpi.dm_queries;           -- Currently running queries
select * from qpi.dm_bre;               -- Active Backup/Restore requests
select * from qpi.dm_blocked_queries;   -- Information about the currently blocked queries
select * from qpi.dm_query_locks;       -- Information about the locks that the queries are holding

select * from qpi.dm_cpu_usage; --  Information about CPU usage.
select * from qpi.dm_mem_usage; --  Information about memory usage.
```

Getting the information about the system performance:
```
select * from qpi.sys_info; --  Get CPU & memory
select * from qpi.volumes;  --  Get info about used and available space on the storage volumes

exec qpi.snapshot_file_stats;   --  Take the file statistics baseline
select * from qpi.file_stats;   --  Get the file stats

exec qpi.snapshot_wait_stats;   --  Take the wait statistics baseline
select * from qpi.wait_stats;   --  Get the wait stats

exec qpi.snapshot_perf_counters;    -- Take the performance counter baseline (required for some perf counters)
select * from qpi.perf_counters;    -- Get the perf counters
```

See more details about the available views and functions in the [QPI API page](doc/Api.md). Find more information how to get the system info in [system information page](doc/SystemInfo.md).

## Performance analysis

QPI library simplifies query performance analysis. Some useful scripts and scenarios are shown in [query performance analysis page](doc/QueryPerformanceAnalisys.md) and [query performance page](doc/QueryStatistics.md).

## File performance

QPI library enables you to find performance of underlying file system - find more information in [file statistics page](doc/FileStatistics.md).

## Installation
QPI library is just a set of views, functions, and utility tables that you can install on your SQL Server or Azure SQL instance. Currently, it supports SQL Server 2016+ and Azure SQL Database.
You can download the source and run it in your database. Choose the version based on your SQL Server version:
- [Azure SQL Managed Instance](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.sql)
- [Azure SQL Database](https://raw.githubusercontent.com/JocaPC/qpi/master/azure-db/qpi.sql)
- [SQL Server 2017](https://raw.githubusercontent.com/JocaPC/qpi/master/sql2017/qpi.sql)
- [SQL Server 2016](https://raw.githubusercontent.com/JocaPC/qpi/master/sql2016/qpi.sql)
 
 All functions, views, and tables are placed in `qpi` schema in your database. You can also remove all functions and views in `qpi` schema using the [cleaning script](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.clean.sql).

 If you are using SQL Agent on SQL Server and Azure SQL Managed you can create a job that periodically snapshot the file and wait statistics using the [QPI Agent job](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.collection.agent.sql).

```

DECLARE @database sysname = <'put the name of the database where QPI procedures are placed'>;
```

> Many views depends on Query Store so make sure that Query store is running on your SQL Server.