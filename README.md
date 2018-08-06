# Query Performance Insights

Query Performance Insights (QPI) is a collection of useful scripts that enable you find what is happening with your SQL Server. It is a set of views and functions that wrap Query Store and Dynamic Management Views.

## Why I need this kind of library

SQL Server/Azure SQL Database provide great views that we can use to analyze query performance (dynamic management views and Query Store views). However, sometime it is hard to see what is happening in the database engine. There are DMVs and Query Store, but when I need to get the answer to the simple questions such as "Did query performance changed after I added an index?" or "How many IOPS do we use?", I need to dig into Query Store schema, think about every single report, or search for some useful query.

This is the reason why I have collected the most useful queries that find information from underlying system views, and wrapped them in a set of useful views. Some usefull resources that I have used:

 - Paul Randal [Wait statistics library](https://www.sqlskills.com/help/waits/), [Wait statistics - tell me where it hurts](https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/), [How to examine IO subsystme latencies](https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/)
 - Aaron Bertrand [Determine system memory](https://www.mssqltips.com/sqlservertip/2393/determine-sql-server-memory-use-by-database-and-object/)
 - Dimitri Furman & [SqlCat team](https://blogs.msdn.microsoft.com/sqlcat/) blog posts, Tim Ford & Louis Davidson [Performance tunning with DMV](https://www.red-gate.com/library/performance-tuning-with-sql-server-dynamic-management-views), 

## System information

QPI library enables you to easily find basic system information that describe your SQL Server instance. This library contains views that you can use to find number of CPU, size of memory, information about files, etc. Find more information in [system information page](doc/SystemInfo.md).
 - qpi.sys_info
 - qpi.dm_mem_usage
 - qpi.dm_cpu_usage
 - qpi.dm_db_mem_usage
 - qpi.dm_mem_plan_cache_info

## Queries

QPI library enables you to find information about the queries that are executed on your SQL Server Database Engine using the following views/functions:
 - qpi.queries
 - qpi.queries_ex
 - qpi.dm_queries
 - qpi.query_texts
 - qpi.dm_blocked_queries
 - qpi.dm_query_locks

 Find more information in [query information page](doc/QueryInfo.md).

## Query Statistics

QPI library enables you to find statistics that can tell you more about query performance using the following views/functions:
 - qpi.query_stats
 - qpi.query_stats_all
 - qpi.query_stats_as_of
 - qpi.query_plan_stats
 - qpi.query_plan_stats_ex
 - qpi.query_plan_stats_all
 - qpi.query_plan_stats_as_of
 - qpi.wait_stats
 - qpi.wait_stats_as_of
 - qpi.snapshot_wait_stats
 - qpi.query_plan_wait_stats
 - qpi.dm_query_stats

Find more information in [query performance page](doc/QueryStatistics.md).

## File performance

QPI library enables you to find performance of underlying file system uisnf the following view/procedure:
 - qpi.file_stats
 - qpi.snapshot_file_stats

Find more information in [file statistics page](doc/FileStatistics.md).

## Performance analysis

QPI library simplifies query performance analysis. Some useful scripts and scenarios are shown in [query performance analysis page](doc/QueryPerformanceAnalisys.md).

## Installation
QPI library is just a set of views functions and utility tables that you can install on your SQL Server or Azure SQL instance. Currently, it supports SQL Server 2016+ and Azure SQL Database.
You can download the [source](https://raw.githubusercontent.com/JocaPC/qpi/master/src/qpi.sql) and run it in your database.

> Many views depends on Query Store so make sure that Query store is running on your SQL Server.