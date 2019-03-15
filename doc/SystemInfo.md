# System information

Query Performance Insight library enables you to monitor resource usage on the server instance.

First, you can find how many resources you have using `qpi.sys_info` view:
```
SELECT *
FROM qpi.sys_info;
```
This view will return you number of CPU and memory that you have on your SQL Server Instance.

You can get more information about the resources that your SQL Server Database Engine is using using `qpi.cpu_usage` and `qpi.mem_usage` views:
```
SELECT * FROM qpi.cpu_usage
SELECT * FROM qpi.mem_usage
SELECT * FROM qpi.mem_plan_cache_info
```

The view `qpi.mem_plan_cache_info` returns detailed information about th ememory usage in plan cache.

Then you can find how much memory is assigned to individual databases using `qpi.db_mem_usage` view:
```
SELECT *
FROM qpi.db_mem_usage;
```
This view will return number of pages in buffer pool for each database and percentage of buffer pool that every database uses.
