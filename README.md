# Query Performance Insights

Query Performance Insights (QPI) is a collection of useful scripts that enable you find what is happening with your SQL Server. It is a set of views and functions that wrap Query Store and Dynamic Management Views.

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
