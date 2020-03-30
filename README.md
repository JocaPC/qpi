# Query Performance Insights

Do you have problem to find the scripts that can help you to easily see what is happening in your database engine? Maybe you hate when you need to dig the scripts from some of your old samples, or to search them on various blog posts or SQL documentation? I had this issue and wanted to collect the most usefull scripts that can help me to analyze query performance on SQL Server/Azure SQL Database.

Query Performance Insights (QPI) is a collection of useful scripts that enable you find what is happening with your SQL Server 2016+ or Azure SQL Database (Single or Managed Instance). This is a set of helper views, functions, and procedures that wrap Query Store and Dynamic Management Objects. See how to install the proper version for your SQL Server in [Installation](#installation) section.

## Why I need this kind of library?

SQL Server/Azure SQL Database provide a lot of views that we can use to analyze query performance (dynamic management object and Query Store views). However, sometime it is hard to see what is happening in the database engine. There are DMVs and Query Store, but when we need to get the answer to the simple questions such as "Are there some queries that are currently blocked", "Did query performance changed after I added an index?" or "How many IOPS do we use?", we need to dig into Query Store schema, think about every single report, or search for some useful query. Also, for most of the views, you would need to read several articles to understand how to interpret the results.

> I need a set of views that I can just execute to see what is happening in my database.

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
select * from qpi.db_queries; -- All database queries recorded in Query Store

select * from qpi.queries;           -- Currently running queries
select * from qpi.bre;               -- Active Backup/Restore requests
select * from qpi.blocked_queries;   -- Information about the currently blocked queries
select * from qpi.query_locks;       -- Information about the locks that the queries are holding

select * from qpi.cpu_usage; --  Information about CPU usage.
select * from qpi.memory; --  Information about memory usage.
```

Getting the information about the system performance:
```
select * from qpi.sys_info; --  Get CPU & memory
select * from qpi.volumes;  --  Get info about used and available space on the storage volumes

exec qpi.snapshot_file_stats;   --  Take the file statistics baseline
<run some query or workload>
select * from qpi.file_stats;   --  Get the file stats

exec qpi.snapshot_wait_stats;   --  Take the wait statistics baseline
<run some query or workload>
select * from qpi.wait_stats;   --  Get the wait stats

exec qpi.snapshot_perf_counters;    -- Take the performance counter baseline (required for some perf counters)
<run some query or workload>
select * from qpi.perf_counters;    -- Get the perf counter values
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
- [Azure SQL Database](https://raw.githubusercontent.com/JocaPC/qpi/master/build/azure-db/qpi.sql)
- [SQL Server 2017](https://raw.githubusercontent.com/JocaPC/qpi/master/build/sql2017/qpi.sql) and higher.
- [SQL Server 2016](https://raw.githubusercontent.com/JocaPC/qpi/master/build/sql2016/qpi.sql)
 
 All functions, views, and tables are placed in `qpi` schema in your database.

> Many views depends on Query Store so make sure that Query store is running on your SQL Server.
 
  You can also remove all functions and views in `qpi` schema using the [cleaning script](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.clean.sql).

 If you are using SQL Agent on SQL Server and Azure SQL Managed you can easily take the snapshots of wait/file statistics. This is needed because file and wait statistics compare the current statistics with the previous ones.
 You can create a job that periodically snapshot the file and wait statistics using the [QPI Agent job](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.collection.agent.sql) script. Before you run this query, set the name of database where you created QPI functionalities:

```
DECLARE @database sysname = <'put the name of the database where QPI procedures are placed'>;

-- the rest of the script is here...
```
