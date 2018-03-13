# Query Performance Insights

Query Performance Insights (QPI) is a collection of useful scripts that enable you find what is happening with your SQL Server. It is a set of views and functions that wrap Query Store and Dynamic Management Views.

## Why I need this kind of library

SQL Server provides great views that we can use to analyze query performance. However, it is hard to see what is happening in the database engine. There are DMVs and Query Store, but when I need to get the answer to the simple questions such as "Did query performance changed after I added an index" or "How many IOPS do we use", I need to dig into Query Store schema, think about every single report, or search for some useful query.

This is the reason why I have collected the most useful queries that find information from underlying system views, and wrapped them in a set of useful views.

## System information

QPI library enables you to easily find basic system information that describe your SQL Server instance. This library contains views that you can use to find number of CPU, size of memory, information about files, etc. Find more information in [system information page](doc/SystemInfo.md).

## Queries

QPI library enables you to find information about the queries that are executed on your SQL Server Database Engine. Find more information in [query information page](doc/QueryInfo.md).

## Query Statistics

QPI library enables you to find statistics that can tell you more about query performance. Find more information in [query performance page](doc/QueryStatistics.md).

## File performance

QPI library enables you to find performance of underlying file system. Find more information in [file statistics page](doc/FileStatistics.md).

## Performance analysis

QPI library simplifies query performance analysis. Some useful scripts and scenarios are shown in  [query performance analysis page](doc/QueryPerformanceAnalisys.md).

## Installation
QPI library is just a set of views functions and utility tables that you can install on your SQL Server or Azure SQL instance. Currently it supports SQL Server 2016+ version.
You can download the [source](src/qpi.sql) and run it in your database.
