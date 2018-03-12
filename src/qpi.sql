--------------------------------------------------------------------------------
--	SQLServer - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------
BEGIN TRY
	EXEC('ALTER TABLE qpi.dm_io_virtual_file_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;
GO
DROP TABLE IF EXISTS qpi.dm_io_virtual_file_stats_snapshot_history;
GO
DROP TABLE IF EXISTS qpi.dm_io_virtual_file_stats_snapshot;
GO
DROP VIEW IF EXISTS qpi.file_stats;
GO
DROP VIEW IF EXISTS qpi.runtime_file_stats;
GO
DROP VIEW IF EXISTS qpi.queries;
GO
DROP VIEW IF EXISTS qpi.queries_ex;
GO
DROP VIEW IF EXISTS qpi.running_queries;
GO
DROP VIEW IF EXISTS qpi.query_texts;
GO
DROP VIEW IF EXISTS qpi.query_stats;
GO
DROP VIEW IF EXISTS qpi.query_stats_ex;
GO
DROP VIEW IF EXISTS qpi.query_wait_stats;
GO
DROP VIEW IF EXISTS qpi.sys_info;
GO
DROP VIEW IF EXISTS qpi.runtime_sys_info;
GO
DROP VIEW IF EXISTS qpi.db_mem_usage;
GO
DROP PROCEDURE IF EXISTS qpi.snapshot_file_stats;
GO
DROP FUNCTION IF EXISTS qpi.decode_options;
GO
DROP FUNCTION IF EXISTS qpi.ago;
GO
DROP FUNCTION IF EXISTS qpi.ago2;
GO
DROP FUNCTION IF EXISTS qpi.us2min;
GO
DROP FUNCTION IF EXISTS qpi.compare_context_settings;
GO
DROP FUNCTION IF EXISTS qpi.runtime_stats_on;
GO
DROP FUNCTION IF EXISTS qpi.query_stats_on;
GO
DROP FUNCTION IF EXISTS qpi.query_stats_ex_on;
GO
DROP FUNCTION IF EXISTS qpi.compare_query_stats_on_intervals;
GO
DROP FUNCTION IF EXISTS qpi.query_plan_stats_diff_between;
GO
DROP FUNCTION IF EXISTS qpi.compare_query_plans;
GO
DROP FUNCTION IF EXISTS qpi.query_wait_stats_on;
GO
DROP FUNCTION IF EXISTS qpi.runtime_file_stats_on;
GO
DROP FUNCTION IF EXISTS qpi.runtime_file_stats_at;
GO
DROP SCHEMA IF EXISTS qpi;
GO

CREATE SCHEMA qpi;
GO
CREATE FUNCTION qpi.us2min(@microseconds bigint)
RETURNS INT
AS BEGIN RETURN ( @microseconds /1000 /1000 /60 ) END;
GO

---
---	SELECT qpi.ago(2,10,15) => GETDATE() - ( 2 days 10 hours 15 min)
---
CREATE FUNCTION qpi.ago(@days tinyint, @hours tinyint, @min tinyint)
RETURNS datetime2
AS BEGIN RETURN DATEADD(day, - @days, 
					DATEADD(hour, - @hours,
						DATEADD(minute, - @min, GETDATE())
						)						
					) END;
GO
---
---	SELECT qpi.ago2(21015) => GETDATE() - ( 2 days 10 hours 15 min)
---
CREATE FUNCTION qpi.ago2(@time int)
RETURNS datetime2
AS BEGIN RETURN DATEADD(DAY, - (@time %10000), 
					DATEADD(HOUR, - (@time /100) %100,
						DATEADD(MINUTE, - (@time %100), GETDATE())
						)						
					) END;
GO
CREATE   FUNCTION qpi.decode_options(@options int)
RETURNS TABLE
RETURN (
SELECT 'DISABLE_DEF_CNST_CHK' = IIF( (1 & @options) = 1, 'ON', 'OFF' )
	, 'IMPLICIT_TRANSACTIONS' = IIF( (2 & @options) = 2, 'ON', 'OFF' )
	, 'CURSOR_CLOSE_ON_COMMIT' = IIF( (4 & @options) = 4, 'ON', 'OFF' ) 
	, 'ANSI_WARNINGS' = IIF( (8 & @options) = 8 , 'ON', 'OFF' )
	, 'ANSI_PADDING' = IIF( (16 & @options) = 16 , 'ON', 'OFF' )
	, 'ANSI_NULLS' = IIF( (32 & @options) = 32 , 'ON', 'OFF' )
	, 'ARITHABORT' = IIF( (64 & @options) = 64 , 'ON', 'OFF' )
	, 'ARITHIGNORE' = IIF( (128 & @options) = 128 , 'ON', 'OFF' )
	, 'QUOTED_IDENTIFIER' = IIF( (256 & @options) = 256 , 'ON', 'OFF' ) 
	, 'NOCOUNT' = IIF( (512 & @options) = 512 , 'ON', 'OFF' )
	, 'ANSI_NULL_DFLT_ON' = IIF( (1024 & @options) = 1024 , 'ON', 'OFF' )
	, 'ANSI_NULL_DFLT_OFF' = IIF( (2048 & @options) = 2048 , 'ON', 'OFF' )
	, 'CONCAT_NULL_YIELDS_NULL' = IIF( (4096 & @options) = 4096 , 'ON', 'OFF' )
	, 'NUMERIC_ROUNDABORT' = IIF( (8192 & @options) = 8192 , 'ON', 'OFF' )
	, 'XACT_ABORT' = IIF( (16384 & @options) = 16384 , 'ON', 'OFF' )
)
GO

CREATE     function qpi.compare_context_settings (@ctx_id1 int, @ctx_id2 int)
returns table
return (
	select a.[key], a.value value1, b.value value2
	from 
	(select [key], value
	from openjson(
	(select *
		from sys.query_context_settings
			cross apply qpi.decode_options(set_options)
		where context_settings_id = @ctx_id1
		for json path, without_array_wrapper)
	)) as a ([key], value)
	join 
	(select [key], value
	from openjson(
	(select *
		from sys.query_context_settings
			cross apply qpi.decode_options(set_options)
		where context_settings_id = @ctx_id2
		for json path, without_array_wrapper)
	)) as b ([key], value)
	on a.[key] = b.[key]
	where a.value <> b.value
);

GO
create view qpi.queries
as
select query_text = query_sql_text, q.query_text_id, query_id, context_settings_id		
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
GO

create view qpi.queries_ex
as
SELECT query_text = query_sql_text, q.query_text_id, query_id, q.context_settings_id,
		o.*		
FROM sys.query_store_query_text t
	JOIN sys.query_store_query q
		ON t.query_text_id = q.query_text_id
		JOIN sys.query_context_settings ctx
			ON q.context_settings_id = ctx.context_settings_id
			CROSS APPLY qpi.decode_options(ctx.set_options) o
GO

create view qpi.query_texts
as
select query_text = query_sql_text, q.query_text_id, 
		queries = string_agg(concat(query_id,'(', context_settings_id,')'),',')		
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
group by query_sql_text, q.query_text_id
GO

CREATE VIEW qpi.running_queries
AS
SELECT  
		query_text = text,
		execution_type_desc = status COLLATE Latin1_General_CS_AS,
		first_execution_time = start_time, last_execution_time = NULL, count_executions = NULL,
		elapsed_time_s = total_elapsed_time /1000.0, 
		cpu_time_s = cpu_time /1000.0, 		 
		logical_io_reads = logical_reads,
		logical_io_writes = writes,
		physical_io_reads = reads, 
		num_physical_io_reads = NULL, 
		clr_time = NULL,
		dop,
		row_count, 
		memory_mb = granted_query_memory *8 /1000, 
		log_bytes = NULL,
		tempdb_space = NULL,
		query_text_id = NULL, query_id = NULL, plan_id = NULL,
		database_id, connection_id, session_id, request_id, command,
		interval_mi = null,
		start_time,
		end_time = null
FROM    sys.dm_exec_requests
		CROSS APPLY sys.dm_exec_sql_text(sql_handle)
WHERE text NOT LIKE '%qpi.running_queries%'
GO

create   function qpi.query_wait_stats_on (@date datetime2)
returns table
as return (
select q.query_id, query_text = t.query_sql_text, rsi.start_time, rsi.end_time,
		ws.wait_stats_id, ws.plan_id, ws.runtime_stats_interval_id,
		ws.wait_category_desc, ws.execution_type_desc, wait_time_s = ws.avg_query_wait_time_ms /1000.0,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time)
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_wait_stats ws on ws.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi on ws.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or @date between rsi.start_time and rsi.end_time 
);
go
CREATE   VIEW qpi.query_wait_stats
AS SELECT * FROM  qpi.query_wait_stats_on (NULL);
GO
go
CREATE   function qpi.query_stats_on (@date datetime2 = NULL)
returns table
as return (
select  query_text = t.query_sql_text,
		execution_type_desc = rs.execution_type_desc COLLATE Latin1_General_CS_AS,
		rs.first_execution_time, rs.last_execution_time, rs.count_executions, 
		duration_s = rs.avg_duration /1000.0 /1000,
		cpu_time_s = rs.avg_cpu_time /1000.0 /1000,
		logical_io_reads = rs.avg_logical_io_reads,
        logical_io_writes = rs.avg_logical_io_writes, 
		physical_io_reads = rs.avg_physical_io_reads,
		num_physical_io_reads = rs.avg_num_physical_io_reads,
		clr_time = rs.avg_clr_time,
		dop = rs.avg_dop,
		memory_mb = rs.avg_query_max_used_memory /8 /1000,
		row_count = rs.avg_rowcount,
		log_bytes = rs.avg_log_bytes_used,
		tempdb_space = rs.avg_tempdb_space_used,
		t.query_text_id, q.query_id, p.plan_id,
		database_id = NULL, connection_id = NULL, session_id = NULL, request_id = NULL, command = NULL,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time),
		rsi.start_time,
		rsi.end_time
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi
		on rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or @date between rsi.start_time and rsi.end_time
union ALL
SELECT  
		query_text = text,
		execution_type_desc = status COLLATE Latin1_General_CS_AS,
		first_execution_time = start_time, last_execution_time = NULL, count_executions = NULL,
		elapsed_time_s = total_elapsed_time /1000.0, 
		cpu_time_s = cpu_time /1000.0, 		 
		logical_io_reads = logical_reads,
		logical_io_writes = writes,
		physical_io_reads = reads, 
		num_physical_io_reads = NULL, 
		clr_time = NULL,
		dop,
		row_count, 
		memory_mb = granted_query_memory *8 /1000, 
		log_bytes = NULL,
		tempdb_space = NULL,
		query_text_id = NULL, query_id = NULL, plan_id = NULL,
		database_id, connection_id, session_id, request_id, command,
		interval_mi = null,
		start_time,
		end_time = null
FROM    sys.dm_exec_requests
		CROSS APPLY sys.dm_exec_sql_text(sql_handle)
where @date is null or start_time < @date
);
GO

CREATE   VIEW qpi.query_stats
AS SELECT * FROM  qpi.query_stats_on (NULL);
GO

CREATE   function qpi.query_stats_ex_on (@date datetime2)
returns table
as return (
select t.query_text_id, q.query_id, query_text = t.query_sql_text, rsi.start_time, rsi.end_time, rs.*,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time)
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi on rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or @date between rsi.start_time and rsi.end_time 
);
GO

CREATE   VIEW qpi.query_stats_ex
AS SELECT * FROM  qpi.query_stats_ex_on (NULL);
GO
create   function qpi.compare_query_stats_on_intervals (@query_id int, @date1 datetime2, @date2 datetime2)
returns table
return (
	select a.[key], a.value value1, b.value value2
	from 
	(select [key], value
	from openjson(
	(select *
		from qpi.query_stats_on(@date1)
		where query_id = @query_id
		for json path, without_array_wrapper)
	)) as a ([key], value)
	join 
	(select [key], value
	from openjson(
	(select *
		from qpi.query_stats_on(@date2)
		where query_id = @query_id
		for json path, without_array_wrapper)
	)) as b ([key], value)
	on a.[key] = b.[key]
	where a.value <> b.value
);
GO
CREATE     FUNCTION qpi.compare_query_plans (@plan_id1 int, @plan_id2 int)
returns table
return (
	select a.[key], a.value value1, b.value value2
	from 
	(select [key], value
	from openjson(
	(select *
		from sys.query_store_plan
		where plan_id = @plan_id1
		for json path, without_array_wrapper)
	)) as a ([key], value)
	join 
	(select [key], value
	from openjson(
	(select *
		from sys.query_store_plan
		where plan_id = @plan_id2
		for json path, without_array_wrapper)
	)) as b ([key], value)
	on a.[key] = b.[key]
	where a.value <> b.value
);
GO

GO

create   function qpi.query_plan_stats_diff_between (@date1 datetime2, @date2 datetime2)
returns table
return (
	select baseline = convert(varchar(16), rsi1.start_time, 20), interval = convert(varchar(16), rsi2.start_time, 20),
		query_text = t.query_sql_text,
		--duration = rs1.avg_duration,
		d_duration = rs2.avg_duration - rs1.avg_duration,
		d_duration_perc = iif(rs2.avg_duration=0, null, ROUND(100*(1 - rs1.avg_duration/rs2.avg_duration),0)),
		--cpu_time = rs1.avg_cpu_time,
		d_cpu_time = rs2.avg_cpu_time - rs1.avg_cpu_time,
		d_cpu_time_perc = iif(rs2.avg_cpu_time=0, null, ROUND(100*(1 - rs1.avg_cpu_time/rs2.avg_cpu_time),0)),
		--physical_io_reads = rs1.avg_physical_io_reads,
		d_physical_io_reads = rs2.avg_physical_io_reads - rs1.avg_physical_io_reads,
		d_physical_io_reads_perc = iif(rs2.avg_physical_io_reads=0, null, ROUND(100*(1 - rs1.avg_physical_io_reads/rs2.avg_physical_io_reads),0)),
		--rs1.avg_log_bytes_used,
		d_log_bytes_used = rs2.avg_log_bytes_used - rs1.avg_log_bytes_used,
		d_log_bytes_used_perc = iif(rs2.avg_log_bytes_used=0, null, ROUND(100*(1 - rs1.avg_log_bytes_used/rs2.avg_log_bytes_used),0)),		
		q.query_text_id, q.query_id, p.plan_id 
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs1 on rs1.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi1 on rs1.runtime_stats_interval_id = rsi1.runtime_stats_interval_id
	left join sys.query_store_runtime_stats rs2 on rs2.plan_id = p.plan_id
	left join sys.query_store_runtime_stats_interval rsi2 on rs2.runtime_stats_interval_id = rsi2.runtime_stats_interval_id
where rsi1.runtime_stats_interval_id <> rsi2.runtime_stats_interval_id
and rsi1.start_time <= @date1 and @date1 < rsi1.end_time
and (@date2 is null or rsi2.start_time <= @date2 and @date2 < rsi2.end_time)
);
GO

CREATE FUNCTION qpi.runtime_stats_on(@date datetime2)
returns table
return (
SELECT  rs.execution_type_desc, 
        rs.count_executions,
        duration_s = rs.avg_duration /1000.0 /1000.0, 
        cpu_time_s = rs.avg_cpu_time /1000.0 /1000.0,
        logical_io_reads_kb = rs.avg_logical_io_reads * 8,
        logical_io_writes_kb = rs.avg_logical_io_writes * 8, 
        physical_io_reads_kb = rs.avg_physical_io_reads * 8, 
        clr_time_s = rs.avg_clr_time /1000.0 /1000.0, 
        max_used_memory_mb = rs.avg_query_max_used_memory * 8.0 /1000,
        num_physical_io_reads = rs.avg_num_physical_io_reads, 
        log_bytes_used_kb = rs.avg_log_bytes_used /1000.0,
        rs.avg_tempdb_space_used,
        start_time = convert(varchar(16), rsi.start_time, 20),
        rs.plan_id,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time)
FROM            sys.query_store_runtime_stats AS rs INNER JOIN
                         sys.query_store_runtime_stats_interval AS rsi ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or (rsi.start_time <= @date and @date < rsi.end_time)
)

GO

-- https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/
CREATE VIEW qpi.file_stats
AS SELECT
	database_name = DB_NAME(vfs.database_id),
    vfs.database_id,
	file_name = [mf].[name],
	size_gb = mf.size * 8 /1024 /1024,
    read_latency_ms =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([io_stall_read_ms] / [num_of_reads]) END,
    write_latency_ms =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([io_stall_write_ms] / [num_of_writes]) END,
    latency =
        CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            THEN 0 ELSE ([io_stall] / ([num_of_reads] + [num_of_writes])) END,
    bytes_per_read =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([num_of_bytes_read] / [num_of_reads]) END,
    bytes_per_write =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([num_of_bytes_written] / [num_of_writes]) END,
    bytes_per_io =
        CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            THEN 0 ELSE
                (([num_of_bytes_read] + [num_of_bytes_written]) /
                ([num_of_reads] + [num_of_writes])) END,
    drive = LEFT ([mf].[physical_name], 2),
	[mf].physical_name
FROM
    sys.dm_io_virtual_file_stats (NULL,NULL) AS [vfs]
JOIN sys.master_files AS [mf]
    ON [vfs].[database_id] = [mf].[database_id]
    AND [vfs].[file_id] = [mf].[file_id]
GO

CREATE TABLE qpi.dm_io_virtual_file_stats_snapshot (
	[database_name] sysname NULL,
	[database_id] [smallint] NOT NULL,
	[file_name] [sysname] NOT NULL,
	[file_id] [smallint] NOT NULL,
	[io_stall_read_ms] [bigint] NOT NULL,
	[io_stall_write_ms] [bigint] NOT NULL,
	[io_stall] [bigint] NOT NULL,
	[num_of_bytes_read] [bigint] NOT NULL,
	[num_of_bytes_written] [bigint] NOT NULL,
	[num_of_reads] [bigint] NOT NULL,
	[num_of_writes] [bigint] NOT NULL,
	title nvarchar(500),
	interval_mi int,
	start_time datetime2 GENERATED ALWAYS AS ROW START,
	end_time datetime2 GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (start_time, end_time),
	PRIMARY KEY (database_id, file_id)  
 ) WITH (SYSTEM_VERSIONING = ON ( HISTORY_TABLE = qpi.dm_io_virtual_file_stats_snapshot_history));
GO
CREATE INDEX ix_file_snapshot_interval_history
	ON qpi.dm_io_virtual_file_stats_snapshot_history(end_time);
GO
CREATE PROCEDURE qpi.snapshot_file_stats @title nvarchar(200) = NULL, @database_name sysname = null, @file_name sysname = null
AS BEGIN
MERGE qpi.dm_io_virtual_file_stats_snapshot AS Target
USING (
	SELECT database_name = DB_NAME(vfs.database_id),vfs.database_id,
		file_name = [mf].[name],[vfs].[file_id],
		[io_stall_read_ms],[io_stall_write_ms],[io_stall],
		[num_of_bytes_read], [num_of_bytes_written],
		[num_of_reads], [num_of_writes]
	FROM sys.dm_io_virtual_file_stats (db_id(@database_name),NULL) AS [vfs]
	JOIN sys.master_files AS [mf] ON
		[vfs].[database_id] = [mf].[database_id] AND [vfs].[file_id] = [mf].[file_id]
		AND (@file_name IS NULL OR [mf].[name] = @file_name)
	) AS Source
ON (Target.file_id = Source.file_id AND Target.database_id = Source.database_id)
WHEN MATCHED THEN
UPDATE SET
	Target.[io_stall_read_ms] = Source.[io_stall_read_ms], -- Target.[io_stall_read_ms],
	Target.[io_stall_write_ms] = Source.[io_stall_write_ms], -- Target.[io_stall_write_ms],
	Target.[io_stall] = Source.[io_stall] ,-- Target.[io_stall],
	Target.[num_of_bytes_read] = Source.[num_of_bytes_read] ,-- Target.[num_of_bytes_read],
	Target.[num_of_bytes_written] = Source.[num_of_bytes_written] ,-- Target.[num_of_bytes_written],
	Target.[num_of_reads] = Source.[num_of_reads] ,-- Target.[num_of_reads],
	Target.[num_of_writes] = Source.[num_of_writes] ,-- Target.[num_of_writes],
	Target.title = ISNULL(@title, CAST( GETDATE() as NVARCHAR(50))),
	Target.interval_mi = DATEDIFF(mi, Target.start_time, GETDATE())
WHEN NOT MATCHED BY TARGET THEN
INSERT (database_name,database_id,file_name,[file_id],
    [io_stall_read_ms],[io_stall_write_ms],[io_stall],
    [num_of_bytes_read], [num_of_bytes_written],
    [num_of_reads], [num_of_writes], title)
VALUES (Source.database_name,Source.database_id,Source.file_name,Source.[file_id],Source.[io_stall_read_ms],Source.[io_stall_write_ms],Source.[io_stall],Source.[num_of_bytes_read],Source.[num_of_bytes_written],Source.[num_of_reads],Source.[num_of_writes],ISNULL(@title, CAST( GETDATE() as NVARCHAR(50)))); 
END
GO
CREATE VIEW qpi.runtime_file_stats
AS SELECT
	database_name,
	file_name,
	size_gb = mf.size * 8 /1024 /1024,
	mbps_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, GETDATE()),
	mbps_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, GETDATE()),
	mb_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024,
	mb_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
	iops = (c.num_of_reads - s.num_of_reads + c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, GETDATE()),
	iops_read = (c.num_of_reads - s.num_of_reads)/ DATEDIFF(second, s.start_time, GETDATE()),
	iops_write = (c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, GETDATE()),
	num_of_reads = c.num_of_reads - s.num_of_reads,
	num_of_writes = c.num_of_writes - s.num_of_writes,
	read_latency_ms =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE ((c.io_stall_read_ms - s.io_stall_read_ms) / (c.num_of_reads - s.num_of_reads)) END,
    write_latency_ms =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((c.io_stall_write_ms - s.io_stall_write_ms) / (c.num_of_writes - s.num_of_writes)) END,
    latency =
        CASE WHEN ( (c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE ((c.io_stall - s.io_stall) / ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes))) END,
    kb_per_read =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE ((c.num_of_bytes_read - s.num_of_bytes_read) / (c.num_of_reads - s.num_of_reads))/1024.0 END,
    kb_per_write =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((c.num_of_bytes_written - s.num_of_bytes_written) / (c.num_of_writes - s.num_of_writes))/1024.0 END,
    kb_per_io =
        CASE WHEN ((c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE
                (((c.num_of_bytes_read - s.num_of_bytes_read) + (c.num_of_bytes_written - s.num_of_bytes_written)) /
                ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes)))/1024.0 END,
	io_stall_read_ms = c.io_stall_read_ms - s.io_stall_read_ms,
	io_stall_write_ms = c.io_stall_write_ms - s.io_stall_write_ms,
	interval_mi = DATEDIFF(minute, s.start_time, GETDATE()),
	c.database_id
FROM sys.dm_io_virtual_file_stats (NULL,NULL) AS c
		JOIN sys.master_files AS [mf]
    	ON [c].[database_id] = [mf].[database_id] AND [c].[file_id] = [mf].[file_id]
		    JOIN qpi.dm_io_virtual_file_stats_snapshot s 
				ON c.database_id = s.database_id AND c.file_id = s.file_id
GO



CREATE FUNCTION qpi.runtime_file_stats_on (@when datetime2(0))
RETURNS TABLE
AS RETURN (SELECT
	c.database_name,
	c.file_name,
	size_gb = mf.size * 8 /1024 /1024,
	mbps_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time),
	mbps_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time),
	mb_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024,
	mb_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
	iops = (c.num_of_reads - s.num_of_reads + c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	iops_read = (c.num_of_reads - s.num_of_reads)/ DATEDIFF(second, s.start_time, c.start_time),
	iops_write = (c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	num_of_reads = c.num_of_reads - s.num_of_reads,
	num_of_writes = c.num_of_writes - s.num_of_writes,
	latency_read_ms =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE ((c.io_stall_read_ms - s.io_stall_read_ms) / (c.num_of_reads - s.num_of_reads)) END,
    latency_write_ms =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((c.io_stall_write_ms - s.io_stall_write_ms) / (c.num_of_writes - s.num_of_writes)) END,
    latency_ms =
        CASE WHEN ( (c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE ((c.io_stall - s.io_stall) / ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes))) END,
    kb_per_read =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE ((c.num_of_bytes_read - s.num_of_bytes_read) / (c.num_of_reads - s.num_of_reads))/1024.0 END,
    kb_per_write =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((c.num_of_bytes_written - s.num_of_bytes_written) / (c.num_of_writes - s.num_of_writes))/1024.0 END,
    kb_per_io =
        CASE WHEN ((c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE
                (((c.num_of_bytes_read - s.num_of_bytes_read) + (c.num_of_bytes_written - s.num_of_bytes_written)) /
                ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes)))/1024.0 END,
	io_stall_read_ms = c.io_stall_read_ms - s.io_stall_read_ms,
	io_stall_write_ms = c.io_stall_write_ms - s.io_stall_write_ms,
	interval_mi = DATEDIFF(minute, s.start_time, c.start_time),
	c.database_id
FROM qpi.dm_io_virtual_file_stats_snapshot FOR SYSTEM_TIME AS OF @when AS c
		JOIN sys.master_files AS [mf]
    	ON [c].[database_id] = [mf].[database_id] AND [c].[file_id] = [mf].[file_id]
		    JOIN qpi.dm_io_virtual_file_stats_snapshot_history s 
			ON c.database_id = s.database_id 
				AND c.file_id = s.file_id 
				AND c.start_time = s.end_time
);
GO
CREATE OR ALTER FUNCTION qpi.runtime_file_stats_at(@milestone nvarchar(100))
RETURNS TABLE
AS RETURN (SELECT
	c.database_name,
	c.file_name,
	mbps_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time),
	mbps_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time),
	mb_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024,
	mb_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
	iops = (c.num_of_reads - s.num_of_reads + c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	iops_read = (c.num_of_reads - s.num_of_reads)/ DATEDIFF(second, s.start_time, c.start_time),
	iops_write = (c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	num_of_reads = c.num_of_reads - s.num_of_reads,
	num_of_writes = c.num_of_writes - s.num_of_writes,
	latency_read_ms =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE ((c.io_stall_read_ms - s.io_stall_read_ms) / (c.num_of_reads - s.num_of_reads)) END,
    latency_write_ms =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((c.io_stall_write_ms - s.io_stall_write_ms) / (c.num_of_writes - s.num_of_writes)) END,
    latency_ms =
        CASE WHEN ( (c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE ((c.io_stall - s.io_stall) / ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes))) END,
    kb_per_read =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE ((c.num_of_bytes_read - s.num_of_bytes_read) / (c.num_of_reads - s.num_of_reads))/1024.0 END,
    kb_per_write =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((c.num_of_bytes_written - s.num_of_bytes_written) / (c.num_of_writes - s.num_of_writes))/1024.0 END,
    kb_per_io =
        CASE WHEN ((c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE
                (((c.num_of_bytes_read - s.num_of_bytes_read) + (c.num_of_bytes_written - s.num_of_bytes_written)) /
                ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes)))/1024.0 END,
	io_stall_read_ms = c.io_stall_read_ms - s.io_stall_read_ms,
	io_stall_write_ms = c.io_stall_write_ms - s.io_stall_write_ms,
	interval_mi = DATEDIFF(minute, s.start_time, c.start_time),
	size_gb = mf.size * 8 /1024 /1024,
	c.database_id
FROM qpi.dm_io_virtual_file_stats_snapshot FOR SYSTEM_TIME ALL AS c
		JOIN sys.master_files AS [mf]
    	ON [c].[database_id] = [mf].[database_id] AND [c].[file_id] = [mf].[file_id]
		    JOIN qpi.dm_io_virtual_file_stats_snapshot_history s 
			ON c.database_id = s.database_id 
				AND c.file_id = s.file_id 
				AND c.start_time = s.end_time
WHERE c.title = @milestone
);
GO
CREATE VIEW qpi.sys_info
AS
SELECT cpu_count, hyperthread_ratio,
physical_cpu_count = cpu_count/hyperthread_ratio,
memory_gb = physical_memory_kb / 1024 /1024,
sqlserver_start_time
FROM sys.dm_os_sys_info
GO
CREATE VIEW qpi.runtime_sys_info
AS
SELECT
	cpu_count,
	[cpu%_sql] = cpu_sql,
	[cpu%_idle] = cpu_idle,
    [cpu%_other] = 100 - cpu_sql - cpu_idle,
	memory_gb = physical_memory_kb / 1024 /1024,
	plan_cache_gb = (SELECT SUM(size_in_bytes)/1024 /1024 FROM sys.dm_exec_cached_plans),
	buffer_pool_gb = (SELECT COUNT_BIG(*) / 128 /1024 FROM sys.dm_os_buffer_descriptors)
   FROM sys.dm_os_sys_info, (
      SELECT  
         cpu_idle = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int'),
         cpu_sql = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')
               FROM (
         SELECT TOP 1 CONVERT(XML, record) AS record 
         FROM sys.dm_os_ring_buffers 
         WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
         AND record LIKE '% %'
		 ORDER BY TIMESTAMP DESC
		 ) as x(record)
		 ) as y
GO      
   



-- https://www.mssqltips.com/sqlservertip/2393/determine-sql-server-memory-use-by-database-and-object/
CREATE VIEW qpi.db_mem_usage
AS
WITH src AS
(
SELECT 
database_id, db_buffer_pages = COUNT_BIG(*)
FROM sys.dm_os_buffer_descriptors
--WHERE database_id BETWEEN 5 AND 32766 --> to exclude system databases
GROUP BY database_id
)
SELECT
[database_name] = CASE [database_id] WHEN 32767 
THEN 'Resource DB' 
ELSE DB_NAME([database_id]) END,
buffer_gb = db_buffer_pages / 128 /1024,
buffer_percent = CONVERT(DECIMAL(6,3), 
db_buffer_pages * 100.0 / (SELECT top 1 cntr_value
							FROM sys.dm_os_performance_counters 
							WHERE RTRIM([object_name]) LIKE '%Buffer Manager'
							AND counter_name = 'Database Pages')
)
FROM src
GO