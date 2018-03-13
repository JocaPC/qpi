# System information

Query Performance Insight library enables you to monitor resource usage on the server instance.

First, you can find how many resources you have using `qpi.sys_info` view:
```
SELECT *
FROM qpi.sys_info;
```
This view will return you number of CPU and memory that you have on your SQL Server Instance.

You can get more information about the resources that your SQL Server Database Engine is using using `qpi.runtime_sys_info` view:
```
SELECT *
FROM qpi.runtime_sys_info;
```

Then you can find how much memory is assigned to individual databases using `qpi.db_mem_usage` view:
```
SELECT *
FROM qpi.db_mem_usage;
```
This view will return number of pages in buffer pool for each database and percentage of buffer pool that every database uses.

Also, you can see how many files you have in your system, including sizes and latencies using `qpi.file_stats` view:
```
SELECT *
FROM qpi.file_stats;
```

Finally, you can take a look at the plan cache usage on your system using `qpi.runtime_plan_cache_info` view:
```
select *
from qpi.runtime_plan_cache_info
```