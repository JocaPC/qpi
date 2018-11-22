--------------------------------------------------------------------------------
--	SQL Server & Azure SQL Managed Instance - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------

CREATE SCHEMA qpi;
GO

CREATE OR ALTER FUNCTION qpi.us2min(@microseconds bigint)
RETURNS INT
AS BEGIN RETURN ( @microseconds /1000 /1000 /60 ) END;
GO

---
---	SELECT qpi.ago(2,10,15) => GETDATE() - ( 2 days 10 hours 15 min)
---
CREATE OR ALTER FUNCTION qpi.ago(@days tinyint, @hours tinyint, @min tinyint)
RETURNS datetime2
AS BEGIN RETURN DATEADD(day, - @days, 
					DATEADD(hour, - @hours,
						DATEADD(minute, - @min, GETDATE())
						)						
					) END;
GO
---
---	SELECT qpi.dhm(21015) => GETDATE() - ( 2 days 10 hours 15 min)
---
CREATE OR ALTER FUNCTION qpi.dhm(@time int)
RETURNS datetime2
AS BEGIN RETURN DATEADD(DAY, - ((@time /10000) %100), 
					DATEADD(HOUR, - (@time /100) %100,
						DATEADD(MINUTE, - (@time %100), GETDATE())
						)						
					) END;
GO
CREATE OR ALTER FUNCTION qpi.decode_options(@options int)
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

CREATE OR ALTER FUNCTION qpi.compare_context_settings (@ctx_id1 int, @ctx_id2 int)
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
CREATE OR ALTER VIEW qpi.queries
as
select	text =  IIF(LEFT(query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( query_sql_text, (PATINDEX( '%)[^,]%', query_sql_text))+1, LEN(query_sql_text))), query_sql_text),
		params = IIF(LEFT(query_sql_text,1) = '(', SUBSTRING( query_sql_text, 0, (PATINDEX( '%)[^,]%', query_sql_text))+1), ''),
		q.query_text_id, query_id, context_settings_id		
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
GO

CREATE OR ALTER VIEW qpi.queries_ex
as
select	text =  IIF(LEFT(query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( query_sql_text, (PATINDEX( '%)[^,]%', query_sql_text))+1, LEN(query_sql_text))), query_sql_text),
		params = IIF(LEFT(query_sql_text,1) = '(', SUBSTRING( query_sql_text, 0, (PATINDEX( '%)[^,]%', query_sql_text))+1), ''),
		q.query_text_id, query_id, q.context_settings_id,
		o.*		
FROM sys.query_store_query_text t
	JOIN sys.query_store_query q
		ON t.query_text_id = q.query_text_id
		JOIN sys.query_context_settings ctx
			ON q.context_settings_id = ctx.context_settings_id
			CROSS APPLY qpi.decode_options(ctx.set_options) o
GO

CREATE OR ALTER VIEW qpi.query_texts
as
select	text =  IIF(LEFT(query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( query_sql_text, (PATINDEX( '%)[^,]%', query_sql_text))+1, LEN(query_sql_text))), query_sql_text),
		params = IIF(LEFT(query_sql_text,1) = '(', SUBSTRING( query_sql_text, 0, (PATINDEX( '%)[^,]%', query_sql_text))+1), ''),
		q.query_text_id, 
		queries = string_agg(concat(query_id,'(', context_settings_id,')'),',')		
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
group by query_sql_text, q.query_text_id
GO

-- The list of currently executing queries that are probably not in Query Store.
CREATE OR ALTER VIEW qpi.dm_queries
AS
SELECT  
		text =  IIF(LEFT(text,1) = '(', TRIM(')' FROM SUBSTRING( text, (PATINDEX( '%)[^,]%', text))+1, LEN(text))), text),
		params = IIF(LEFT(text,1) = '(', SUBSTRING( text, 0, (PATINDEX( '%)[^,]%', text))+1), ''),
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
WHERE text NOT LIKE '%qpi.dm_queries%'
GO

CREATE OR ALTER VIEW qpi.dm_bre
AS
SELECT r.command,percent_complete = CONVERT(NUMERIC(6,2),r.percent_complete)
,CONVERT(VARCHAR(20),DATEADD(ms,r.estimated_completion_time,GetDate()),20) AS ETA,
CONVERT(NUMERIC(10,2),r.total_elapsed_time/1000.0/60.0) AS elapsed_mi,
CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0/60.0) AS eta_h,
CONVERT(VARCHAR(1000),(SELECT SUBSTRING(text,r.statement_start_offset/2,
CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)
FROM sys.dm_exec_sql_text(sql_handle))) AS query,r.session_id
FROM sys.dm_exec_requests r WHERE command IN ('RESTORE DATABASE','BACKUP DATABASE') 
GO

CREATE OR ALTER VIEW qpi.dm_query_locks
AS
SELECT   
	text = q.text,
	session_id = q.session_id,
	tl.request_owner_type,
	tl.request_status,
	tl.request_mode,
	tl.request_type,
	locked_object_id = obj.object_id,
	locked_object_schema = SCHEMA_NAME(obj.schema_id),
	locked_object_name = obj.name,
	locked_object_type = obj.type_desc,
	locked_resource_type = tl.resource_type,
	locked_resource_db = DB_NAME(tl.resource_database_id),
	q.request_id,
	tl.resource_associated_entity_id
FROM qpi.dm_queries q
	JOIN sys.dm_tran_locks as tl
		ON q.session_id = tl.request_session_id and q.request_id = tl.request_request_id 
		LEFT JOIN sys.partitions p ON p.hobt_id = tl.resource_associated_entity_id
			LEFT JOIN sys.objects obj ON p.object_id = obj.object_id
GO

------------------------------------------------------------------------------------
--	Query performance statistics.
------------------------------------------------------------------------------------

-- Returns the stats for the currently running queries.
CREATE   VIEW qpi.dm_query_stats
AS
SELECT  
		text =  IIF(LEFT(t.text,1) = '(', TRIM(')' FROM SUBSTRING( t.text, (PATINDEX( '%)[^,]%', t.text))+1, LEN(t.text))), t.text),
		params = IIF(LEFT(t.text,1) = '(', SUBSTRING( t.text, 0, (PATINDEX( '%)[^,]%', t.text))+1), ''),
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
		memory_mb = granted_query_memory *8 /1024, 
		log_bytes = NULL,
		tempdb_space = NULL,
		query_text_id = NULL, query_id = NULL, plan_id = NULL,
		database_id, connection_id, session_id, request_id, command,
		interval_mi = null,
		start_time,
		end_time = null
FROM    sys.dm_exec_requests
		CROSS APPLY sys.dm_exec_sql_text(sql_handle) t
GO

CREATE OR ALTER VIEW qpi.dm_blocked_queries
AS
SELECT   
	text = blocked.text,
	session_id = blocked.session_id,
	blocked_by_session_id = conn.session_id,
	blocked_by_query = last_query.text,
	wait_time_s = w.wait_duration_ms /1000,
	w.wait_type,
	locked_object_id = obj.object_id,
	locked_object_schema = SCHEMA_NAME(obj.schema_id),
	locked_object_name = obj.name,
	locked_object_type = obj.type_desc,
	locked_resource_type = tl.resource_type,
	locked_resource_db = DB_NAME(tl.resource_database_id),
	tl.request_mode,
	tl.request_type,
	tl.request_status,
	tl.request_owner_type,
	w.resource_description
FROM qpi.dm_queries blocked
	INNER JOIN sys.dm_os_waiting_tasks w
	ON blocked.session_id = w.session_id 
		INNER JOIN sys.dm_exec_connections conn
		ON conn.session_id =  w.blocking_session_id
			CROSS APPLY sys.dm_exec_sql_text(conn.most_recent_sql_handle) AS last_query 
	LEFT JOIN sys.dm_tran_locks as tl
	 ON tl.lock_owner_address = w.resource_address 
	 LEFT JOIN sys.partitions p ON p.hobt_id = tl.resource_associated_entity_id
		LEFT JOIN sys.objects obj ON p.object_id = obj.object_id
WHERE w.session_id <> w.blocking_session_id
GO

/*******************************************************************************
*	Wait statistics
*******************************************************************************/

CREATE TABLE qpi.dm_os_wait_stats_snapshot
	(
	[category_id] tinyint NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[waiting_tasks_count] [bigint] NOT NULL,
	[wait_time_ms] [bigint] NOT NULL,
	[max_wait_time_ms] [bigint] NOT NULL,
	[signal_wait_time_ms] [bigint] NOT NULL,
	[title] [nvarchar](50),
	start_time datetime2 GENERATED ALWAYS AS ROW START,
	end_time datetime2 GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (start_time, end_time),
	PRIMARY KEY (wait_type)  
 ) WITH (SYSTEM_VERSIONING = ON ( HISTORY_TABLE = qpi.dm_os_wait_stats_snapshot_history));
GO
CREATE INDEX ix_dm_os_wait_stats_snapshot
	ON qpi.dm_os_wait_stats_snapshot_history(end_time);
GO

CREATE PROCEDURE qpi.snapshot_wait_stats @title nvarchar(200) = NULL
AS BEGIN
MERGE qpi.dm_os_wait_stats_snapshot AS Target
USING (
	SELECT
	category_id = CASE
		WHEN wait_type = 'Unknown'				THEN 0
		WHEN wait_type = 'SOS_SCHEDULER_YIELD'	THEN 1
		WHEN wait_type = 'SOS_WORK_DISPATCHER'	THEN 1
		WHEN wait_type = 'THREADPOOL'			THEN 2
		WHEN wait_type LIKE 'LCK_M_%'			THEN 3
		WHEN wait_type LIKE 'LATCH_%'			THEN 4
		WHEN wait_type LIKE 'PAGELATCH_%'		THEN 5
		WHEN wait_type LIKE 'PAGEIOLATCH_%'		THEN 6
		WHEN wait_type = 'RESOURCE_SEMAPHORE_QUERY_COMPILE'
												THEN 7
		WHEN wait_type LIKE 'CLR%'				THEN 8
		WHEN wait_type LIKE 'SQLCLR%'			THEN 8
		WHEN wait_type LIKE 'DBMIRROR%'			THEN 9
		WHEN wait_type LIKE 'XACT%'				THEN 10
		WHEN wait_type LIKE 'DTC%'				THEN 10
		WHEN wait_type LIKE 'TRAN_MARKLATCH_%'	THEN 10
		WHEN wait_type = 'TRANSACTION_MUTEX'	THEN 10
		WHEN wait_type LIKE 'MSQL_XACT_%'		THEN 10
		WHEN wait_type LIKE 'SLEEP_%'			THEN 11
		WHEN wait_type IN ('LAZYWRITER_SLEEP', 'SQLTRACE_BUFFER_FLUSH',
							'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES',
							'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT',
							'REQUEST_FOR_DEADLOCK_SEARCH', 'LOGMGR_QUEUE',
							'ONDEMAND_TASK_QUEUE', 'CHECKPOINT_QUEUE', 'XE_TIMER_EVENT')
												THEN 11
		WHEN wait_type LIKE 'PREEMPTIVE_%'		THEN 12
		WHEN wait_type LIKE 'BROKER_%' AND wait_type <> 'BROKER_RECEIVE_WAITFOR'
												THEN 13
		WHEN wait_type IN ('LOGMGR', 'LOGBUFFER', 'LOGMGR_RESERVE_APPEND', 'LOGMGR_FLUSH',
							'LOGMGR_PMM_LOG', 'CHKPT', 'WRITELOG')
														THEN 14
		WHEN wait_type IN ('ASYNC_NETWORK_IO', 'NET_WAITFOR_PACKET', 'PROXY_NETWORK_IO', 
							'EXTERNAL_SCRIPT_NETWORK_IO')
														THEN 15														
		WHEN wait_type IN ('CXPACKET', 'EXCHANGE')
														THEN 16
		WHEN wait_type IN ('RESOURCE_SEMAPHORE', 'CMEMTHREAD', 'CMEMPARTITIONED', 'EE_PMOLOCK',
							'MEMORY_ALLOCATION_EXT', 'RESERVED_MEMORY_ALLOCATION_EXT', 'MEMORY_GRANT_UPDATE')
														THEN 17
		WHEN wait_type IN ('WAITFOR', 'WAIT_FOR_RESULTS', 'BROKER_RECEIVE_WAITFOR')
														THEN 18
		WHEN wait_type IN ('TRACEWRITE', 'SQLTRACE_LOCK', 'SQLTRACE_FILE_BUFFER', 'SQLTRACE_FILE_WRITE_IO_COMPLETION', 'SQLTRACE_FILE_READ_IO_COMPLETION', 'SQLTRACE_PENDING_BUFFER_WRITERS', 'SQLTRACE_SHUTDOWN', 'QUERY_TRACEOUT', 'TRACE_EVTNOTIFF')
														THEN 19
		WHEN wait_type IN ('FT_RESTART_CRAWL', 'FULLTEXT GATHERER', 'MSSEARCH', 'FT_METADATA_MUTEX', 'FT_IFTSHC_MUTEX', 'FT_IFTSISM_MUTEX', 'FT_IFTS_RWLOCK', 'FT_COMPROWSET_RWLOCK', 'FT_MASTER_MERGE', 'FT_PROPERTYLIST_CACHE', 'FT_MASTER_MERGE_COORDINATOR', 'PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC')
														THEN 20
		WHEN wait_type IN ('ASYNC_IO_COMPLETION', 'IO_COMPLETION', 'BACKUPIO',
							'WRITE_COMPLETION', 'IO_QUEUE_LIMIT', 'IO_RETRY')
														THEN 21
		WHEN wait_type IN ('REPLICA_WRITES', 'FCB_REPLICA_WRITE', 'FCB_REPLICA_READ', 'PWAIT_HADRSIM')
														THEN 22
		WHEN wait_type LIKE 'SE_REPL_%'					THEN 22
		WHEN wait_type LIKE 'REPL_%'					THEN 22
		WHEN wait_type LIKE 'HADR_%'
		AND	 wait_type <> 'HADR_THROTTLE_LOG_RATE_GOVERNOR'
														THEN 22
		WHEN wait_type LIKE 'PWAIT_HADR_%'				THEN 22
		WHEN wait_type IN ('LOG_RATE_GOVERNOR', 'POOL_LOG_RATE_GOVERNOR',
							'HADR_THROTTLE_LOG_RATE_GOVERNOR', 'INSTANCE_LOG_RATE_GOVERNOR')
														THEN 23		
		ELSE NULL
	END,
	wait_type = [wait_type] COLLATE Latin1_General_100_BIN2,
	[waiting_tasks_count],
	[wait_time_ms],
	[max_wait_time_ms],
	[signal_wait_time_ms]
	from sys.dm_os_wait_stats
	-- see: https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/
	-- Last updated June 13, 2018
	where [wait_type] NOT IN (
        -- These wait types are almost 100% never a problem and so they are
        -- filtered out to avoid them skewing the results. Click on the URL
        -- for more information.
        N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
        N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
        N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
 
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
 
        N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
        N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
 
        -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
 
        N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
        N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
        )
	and waiting_tasks_count > 0
	) AS Source
ON (Target.wait_type = Source.wait_type)
WHEN MATCHED THEN
UPDATE SET
	-- https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-query-store-wait-stats-transact-sql?view=sql-server-2017#wait-categories-mapping-table
	Target.[category_id] = Source.[category_id],
	Target.[wait_type] = Source.[wait_type],
	Target.[waiting_tasks_count] = Source.[waiting_tasks_count],
	Target.[wait_time_ms] = Source.[wait_time_ms],
	Target.[max_wait_time_ms] = Source.[max_wait_time_ms],
	Target.[signal_wait_time_ms] = Source.[signal_wait_time_ms],
	Target.title = ISNULL(@title, CONVERT(VARCHAR(30), GETDATE(), 20))
WHEN NOT MATCHED BY TARGET THEN
INSERT (category_id,
	[wait_type],
	[waiting_tasks_count],
	[wait_time_ms],
	[max_wait_time_ms],
	[signal_wait_time_ms], title)
VALUES (Source.category_id, Source.[wait_type],Source.[waiting_tasks_count],
		Source.[wait_time_ms], Source.[max_wait_time_ms],
		Source.[signal_wait_time_ms],
		ISNULL(@title, CAST( GETDATE() as NVARCHAR(50))));

DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR);
 
END
GO

create or alter  function qpi.wait_stats_as_of(@date datetime2)
returns table
as return (
select
	category = CASE category_id
				WHEN 0 THEN 'Unknown'
				WHEN 1 THEN 'CPU'
				WHEN 2 THEN 'Worker Thread'
				WHEN 3 THEN 'Lock'
				WHEN 4 THEN 'Latch'
				WHEN 5 THEN 'Buffer Latch'
				WHEN 6 THEN 'Buffer IO'
				WHEN 7 THEN 'Compilation'
				WHEN 8 THEN 'SQL CLR'
				WHEN 9 THEN 'Mirroring'
				WHEN 10 THEN 'Transaction'
				WHEN 11 THEN 'Idle'
				WHEN 12 THEN 'Preemptive'
				WHEN 13 THEN 'Service Broker'
				WHEN 14 THEN 'Tran Log IO'
				WHEN 15 THEN 'Network IO'
				WHEN 16 THEN 'Parallelism'
				WHEN 17 THEN 'Memory'
				WHEN 18 THEN 'User Wait'
				WHEN 19 THEN 'Tracing'
				WHEN 20 THEN 'Full Text Search'
				WHEN 21 THEN 'Other Disk IO'
				WHEN 22 THEN 'Replication'
				WHEN 23 THEN 'Log Rate Governor'
			END,
			wait_type,
			wait_time_s = wait_time_ms /1000, 
			avg_wait_time = wait_time_ms / DATEDIFF(ms, start_time, GETUTCDATE()),
			signal_wait_time_s = signal_wait_time_ms /1000, 
			avg_signal_wait_time = signal_wait_time_ms / DATEDIFF(ms, start_time, GETUTCDATE()),
			max_wait_time_s = max_wait_time_ms /1000,
			category_id,
			snapshot_time = start_time
from qpi.dm_os_wait_stats_snapshot for system_time all rsi
where @date is null or @date between rsi.start_time and rsi.end_time 
);
go
CREATE OR ALTER
VIEW qpi.wait_stats
AS SELECT * FROM  qpi.wait_stats_as_of(GETDATE());
GO

CREATE OR ALTER
VIEW qpi.wait_stats_all
AS SELECT * FROM  qpi.wait_stats_as_of(null);
GO

create or alter
function qpi.query_plan_wait_stats_as_of(@date datetime2)
	returns table
as return (
select	 
		text =  IIF(LEFT(t.query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^,]%', t.query_sql_text))+1, LEN(t.query_sql_text))), t.query_sql_text),
		params = IIF(LEFT(t.query_sql_text,1) = '(', SUBSTRING( t.query_sql_text, 0, (PATINDEX( '%)[^,]%', t.query_sql_text))+1), ''),
		category = ws.wait_category_desc, wait_time_s = ws.avg_query_wait_time_ms /1000.0,
		q.query_id, ws.plan_id, ws.execution_type_desc, 
		rsi.start_time, rsi.end_time,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time),
		ws.runtime_stats_interval_id, ws.wait_stats_id
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_wait_stats ws on ws.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi on ws.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or @date between rsi.start_time and rsi.end_time 
);
go

CREATE OR ALTER
VIEW qpi.query_plan_wait_stats
AS SELECT * FROM  qpi.query_plan_wait_stats_as_of(GETDATE());
GO

create or alter
function qpi.query_wait_stats_as_of(@date datetime2)
	returns table
as return (
	select	
		text = min(text),
		params = min(params),
		category, wait_time_s = sum(wait_time_s),
		execution_type_desc, 
		start_time = min(start_time), end_time = min(end_time),
		interval_mi = min(interval_mi)
from qpi.query_plan_wait_stats_as_of(@date)
group by query_id, category, execution_type_desc
);
go

create or alter
view qpi.query_wait_stats
as select * from qpi.query_wait_stats_as_of(getdate())
go

create or alter
view qpi.query_wait_stats_all
as select * from qpi.query_wait_stats_as_of(null)
go

CREATE OR ALTER  function qpi.query_plan_stats_as_of(@date datetime2)
returns table
as return (
select	t.query_text_id, q.query_id, 
		text =  IIF(LEFT(t.query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^,]%', t.query_sql_text))+1, LEN(t.query_sql_text))), t.query_sql_text),
		params = IIF(LEFT(t.query_sql_text,1) = '(', SUBSTRING( t.query_sql_text, 0, (PATINDEX( '%)[^,]%', t.query_sql_text))+1), ''),
		rs.plan_id,
		rs.execution_type_desc, 
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
		end_time = convert(varchar(16), rsi.end_time, 20),
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time),
		q.context_settings_id
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi 
			on rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where (@date is null or @date between rsi.start_time and rsi.end_time)

);
GO
-- END wait statistics


CREATE OR ALTER VIEW qpi.query_plan_stats
AS SELECT * FROM qpi.query_plan_stats_as_of(GETDATE());
GO

CREATE OR ALTER VIEW qpi.query_plan_stats_all
AS SELECT * FROM qpi.query_plan_stats_as_of(NULL);
GO


-- Returns all query plan statistics without currently running values.
CREATE   function qpi.query_plan_stats_ex_as_of(@date datetime2)
returns table
as return (
select	q.query_id, 
		text =  IIF(LEFT(t.query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^,]%', t.query_sql_text))+1, LEN(t.query_sql_text))), t.query_sql_text),
		params = IIF(LEFT(t.query_sql_text,1) = '(', SUBSTRING( t.query_sql_text, 0, (PATINDEX( '%)[^,]%', t.query_sql_text))+1), ''),
		t.query_text_id, rsi.start_time, rsi.end_time,
		rs.*,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time)
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi on rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or @date between rsi.start_time and rsi.end_time 
);
GO

CREATE   VIEW qpi.query_plan_stats_ex
AS SELECT * FROM qpi.query_plan_stats_ex_as_of(GETDATE());
GO

-- the most important view: query statistics:
GO
-- Returns statistics about all queries as of specified time.
CREATE FUNCTION qpi.query_stats_as_of(@date datetime2)
returns table
return (

WITH query_stats as (
SELECT	qps.query_id, execution_type_desc,
		duration_s = SUM(duration_s),
		count_executions = SUM(count_executions),
		cpu_time_s = SUM(cpu_time_s),
		logical_io_reads_kb = SUM(logical_io_reads_kb),
		logical_io_writes_kb = SUM(logical_io_writes_kb),
		physical_io_reads_kb = SUM(physical_io_reads_kb),
		clr_time_s = SUM(clr_time_s),
		num_physical_io_reads = SUM(num_physical_io_reads),
		log_bytes_used_kb = SUM(log_bytes_used_kb),
		avg_tempdb_space_used = SUM(avg_tempdb_space_used),
		start_time = MIN(start_time),
		interval_mi = MIN(interval_mi)
FROM qpi.query_plan_stats_as_of(@date) qps 
GROUP BY query_id, execution_type_desc
)
SELECT  text =  IIF(LEFT(t.query_sql_text,1) = '(', TRIM(')' FROM SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^,]%', t.query_sql_text))+1, LEN(t.query_sql_text))), t.query_sql_text),
		params = IIF(LEFT(t.query_sql_text,1) = '(', SUBSTRING( t.query_sql_text, 0, (PATINDEX( '%)[^,]%', t.query_sql_text))+1), ''),
		qs.*,
		t.query_text_id
FROM query_stats qs
	join sys.query_store_query q  
	on q.query_id = qs.query_id
	join sys.query_store_query_text t
	on q.query_text_id = t.query_text_id
	
)
GO

CREATE   VIEW qpi.query_stats
AS SELECT * FROM  qpi.query_stats_as_of(GETDATE());
GO
CREATE   VIEW qpi.query_stats_all
AS SELECT * FROM  qpi.query_stats_as_of(NULL);
GO

--- Query comparison

create   function qpi.compare_query_stats_on_intervals (@query_id int, @date1 datetime2, @date2 datetime2)
returns table
return (
	select a.[key], a.value value1, b.value value2
	from 
	(select [key], value
	from openjson(
	(select *
		from qpi.query_stats_as_of(@date1)
		where query_id = @query_id
		for json path, without_array_wrapper)
	)) as a ([key], value)
	join 
	(select [key], value
	from openjson(
	(select *
		from qpi.query_stats_as_of(@date2)
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

create   function qpi.query_plan_stats_diff_on_intervals (@date1 datetime2, @date2 datetime2)
returns table
return (
	select baseline = convert(varchar(16), rsi1.start_time, 20), interval = convert(varchar(16), rsi2.start_time, 20),
		query_text = t.query_sql_text,
		d_duration = rs2.avg_duration - rs1.avg_duration,
		d_duration_perc = iif(rs2.avg_duration=0, null, ROUND(100*(1 - rs1.avg_duration/rs2.avg_duration),0)),
		d_cpu_time = rs2.avg_cpu_time - rs1.avg_cpu_time,
		d_cpu_time_perc = iif(rs2.avg_cpu_time=0, null, ROUND(100*(1 - rs1.avg_cpu_time/rs2.avg_cpu_time),0)),
		d_physical_io_reads = rs2.avg_physical_io_reads - rs1.avg_physical_io_reads,
		d_physical_io_reads_perc = iif(rs2.avg_physical_io_reads=0, null, ROUND(100*(1 - rs1.avg_physical_io_reads/rs2.avg_physical_io_reads),0)),
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
GO
/*
-- https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/
*/

CREATE TABLE qpi.dm_io_virtual_file_stats_snapshot (
	[db_name] sysname NULL,
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
CREATE PROCEDURE qpi.snapshot_file_stats @title nvarchar(200) = NULL, @db_name sysname = null, @file_name sysname = null
AS BEGIN
MERGE qpi.dm_io_virtual_file_stats_snapshot AS Target
USING (
	SELECT db_name = DB_NAME(vfs.database_id),vfs.database_id,
		file_name = [mf].[name],[vfs].[file_id],
		[io_stall_read_ms],[io_stall_write_ms],[io_stall],
		[num_of_bytes_read], [num_of_bytes_written],
		[num_of_reads], [num_of_writes]
	FROM sys.dm_io_virtual_file_stats (db_id(@db_name),NULL) AS [vfs]
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
	Target.title = ISNULL(@title, CONVERT(VARCHAR(30), GETDATE(), 20)),
	Target.interval_mi = DATEDIFF(mi, Target.start_time, GETDATE())
WHEN NOT MATCHED BY TARGET THEN
INSERT (db_name,database_id,file_name,[file_id],
    [io_stall_read_ms],[io_stall_write_ms],[io_stall],
    [num_of_bytes_read], [num_of_bytes_written],
    [num_of_reads], [num_of_writes], title)
VALUES (Source.db_name,Source.database_id,Source.file_name,Source.[file_id],Source.[io_stall_read_ms],Source.[io_stall_write_ms],Source.[io_stall],Source.[num_of_bytes_read],Source.[num_of_bytes_written],Source.[num_of_reads],Source.[num_of_writes],ISNULL(@title, CAST( GETDATE() as NVARCHAR(50)))); 
END
GO
CREATE OR ALTER VIEW qpi.file_stats
AS SELECT
	db_name = DB_NAME(mf.database_id),
	file_name = mf.name,
	size_gb = CAST(ROUND(mf.size /1024.0 /1024 * 8, 1) AS NUMERIC(10,1)),
	throughput_mbps = CAST((c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, GETDATE()) AS numeric(10,2))
		 + CAST((c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, GETDATE()) AS numeric(10,2)),
	read_mbps = CAST((c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, GETDATE()) AS numeric(10,2)),
	write_mbps = CAST((c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, GETDATE()) AS numeric(10,2)),
	iops = (c.num_of_reads - s.num_of_reads + c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, GETDATE()),
	read_iops = (c.num_of_reads - s.num_of_reads)/ DATEDIFF(second, s.start_time, GETDATE()),
	write_iops = (c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, GETDATE()),
	latency_ms =
        CASE WHEN ( (c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall - s.io_stall) / ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes)), 1) AS numeric(5,1))) END,
    read_latency_ms =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall_read_ms - s.io_stall_read_ms) / (c.num_of_reads - s.num_of_reads), 1) AS numeric(5,1))) END,
    write_latency_ms =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall_write_ms - s.io_stall_write_ms) / (c.num_of_writes - s.num_of_writes), 1) AS numeric(5,1))) END,
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
	read_mb = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024,
	write_mb = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
	num_of_reads = c.num_of_reads - s.num_of_reads,
	num_of_writes = c.num_of_writes - s.num_of_writes,
	interval_mi = DATEDIFF(minute, s.start_time, GETDATE()),
	c.database_id
FROM sys.master_files AS [mf]
		OUTER APPLY sys.dm_io_virtual_file_stats ([mf].[database_id], [mf].[file_id]) AS c
		--LEFT JOIN sys.dm_io_virtual_file_stats (NULL, NULL) AS c ON [c].[database_id] = mf.database_id AND [c].[file_id] = mf.file_id
		    LEFT JOIN qpi.dm_io_virtual_file_stats_snapshot s 
				ON c.database_id = s.database_id AND c.file_id = s.file_id
GO

CREATE OR ALTER VIEW qpi.db_file_stats
AS SELECT * FROM qpi.file_stats
	WHERE db_name = DB_NAME()
GO

CREATE OR ALTER FUNCTION qpi.file_stats_as_of(@when datetime2(0))
RETURNS TABLE
AS RETURN (SELECT
	db_name = DB_NAME(mf.database_id),
	file_name = mf.name,
	size_gb = CAST(ROUND(mf.size /1024.0 /1024 * 8, 1) AS NUMERIC(10,1)),
	throughput_mbps = CAST((c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2))
					+ CAST((c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2)),
	read_mbps = CAST((c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2)),
	write_mbps = CAST((c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2)),
	iops = (c.num_of_reads - s.num_of_reads + c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	read_iops = (c.num_of_reads - s.num_of_reads)/ DATEDIFF(second, s.start_time, c.start_time),
	write_iops = (c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
		latency_ms =
        CASE WHEN ( (c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall - s.io_stall) / ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes)), 1) AS numeric(5,1))) END,
	latency_read_ms =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall_read_ms - s.io_stall_read_ms) / (c.num_of_reads - s.num_of_reads), 1) AS numeric(5,1))) END,
    latency_write_ms =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((CAST(ROUND(
							1.0 * (c.io_stall_write_ms - s.io_stall_write_ms) /
									 (c.num_of_writes - s.num_of_writes)
									 , 1) AS numeric(5,1)))) END,
	num_of_reads = c.num_of_reads - s.num_of_reads,
	num_of_writes = c.num_of_writes - s.num_of_writes,
	io_mb = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024
				  + (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
	read_mb = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024,
	writte_mb = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
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
FROM sys.master_files AS [mf]
		LEFT JOIN qpi.dm_io_virtual_file_stats_snapshot FOR SYSTEM_TIME AS OF @when AS c
    	ON [c].[database_id] = [mf].[database_id] AND [c].[file_id] = [mf].[file_id]
		    LEFT JOIN qpi.dm_io_virtual_file_stats_snapshot_history s 
			ON c.database_id = s.database_id 
				AND c.file_id = s.file_id 
				AND c.start_time = s.end_time
);
GO
CREATE OR ALTER FUNCTION qpi.file_stats_at(@milestone nvarchar(100))
RETURNS TABLE
AS RETURN (SELECT
	db_name = DB_NAME(mf.database_id),
	file_name = mf.name,
	io_mbps = CAST((c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2))
			+ CAST((c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2)),
	read_mbps = CAST((c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2)),
	write_mbps = CAST((c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024.0 / DATEDIFF(second, s.start_time, c.start_time) AS numeric(10,2)),
	latency_ms =
        CASE WHEN ( (c.num_of_reads - s.num_of_reads) = 0 AND (c.num_of_writes - s.num_of_writes) = 0)
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall - s.io_stall) / ((c.num_of_reads - s.num_of_reads) + (c.num_of_writes - s.num_of_writes)), 1) AS numeric(5,1))) END,
    latency_read_ms =
        CASE WHEN (c.num_of_reads - s.num_of_reads) = 0
            THEN 0 ELSE (CAST(ROUND(1.0 * (c.io_stall_read_ms - s.io_stall_read_ms) / (c.num_of_reads - s.num_of_reads), 1) AS numeric(5,1))) END,
    latency_write_ms =
        CASE WHEN (c.num_of_writes - s.num_of_writes) = 0
            THEN 0 ELSE ((CAST(ROUND(
							1.0 * (c.io_stall_write_ms - s.io_stall_write_ms) /
									 (c.num_of_writes - s.num_of_writes)
									 , 1) AS numeric(5,1)))) END,
	iops = (c.num_of_reads - s.num_of_reads + c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	iops_read = (c.num_of_reads - s.num_of_reads)/ DATEDIFF(second, s.start_time, c.start_time),
	iops_write = (c.num_of_writes - s.num_of_writes)/ DATEDIFF(second, s.start_time, c.start_time),
	mb_read = (c.num_of_bytes_read - s.num_of_bytes_read)/1024.0/1024,
	mb_written = (c.num_of_bytes_written - s.num_of_bytes_written)/1024.0/1024,
	num_of_reads = c.num_of_reads - s.num_of_reads,
	num_of_writes = c.num_of_writes - s.num_of_writes,
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
	size_gb = CAST(ROUND(mf.size /1024.0 /1024 * 8, 1) AS NUMERIC(10,1)),
	c.database_id
FROM sys.master_files AS [mf]
		LEFT JOIN qpi.dm_io_virtual_file_stats_snapshot FOR SYSTEM_TIME ALL AS c
    	ON [c].[database_id] = [mf].[database_id] AND [c].[file_id] = [mf].[file_id]
		    JOIN qpi.dm_io_virtual_file_stats_snapshot_history s -- Do not remove this join - needed for calculating diff with previous. #Alzheimer
			ON c.database_id = s.database_id 
				AND c.file_id = s.file_id 
				AND c.start_time = s.end_time
WHERE c.title = @milestone
);
GO

CREATE VIEW qpi.file_stats_snapshots
AS
SELECT DISTINCT snapshot_name = title, start_time, end_time
FROM qpi.dm_io_virtual_file_stats_snapshot FOR SYSTEM_TIME ALL
GO

CREATE OR ALTER VIEW qpi.dm_volumes
AS
SELECT	volume_mount_point,
		used_gb = MIN(total_bytes / 1024 / 1024 / 1024),
		available_gb = MIN(available_bytes / 1024 / 1024 / 1024),
		total_gb = MIN((total_bytes+available_bytes) / 1024 / 1024 / 1024)
FROM sys.master_files AS f  
CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id)
GROUP BY volume_mount_point;
GO

CREATE VIEW qpi.sys_info
AS
SELECT cpu_count,
	memory_gb = (select process_memory_limit_mb - non_sos_mem_gap_mb FROM sys.dm_os_job_object) /1024,
	sqlserver_start_time,
	hyperthread_ratio,
	physical_cpu_count = cpu_count/hyperthread_ratio
FROM sys.dm_os_sys_info
GO
CREATE VIEW qpi.dm_cpu_usage
AS
SELECT
	cpu_count,
	[sql_perc] = cpu_sql,
	[idle_perc] = cpu_idle,
    [other_perc] = 100 - cpu_sql - cpu_idle
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
   
CREATE VIEW qpi.dm_mem_plan_cache_info
AS
SELECT  cached_object = objtype,
        memory_gb = SUM(size_in_bytes /1024 /1024 /1024),
		plan_count = COUNT_BIG(*)
    FROM sys.dm_exec_cached_plans
    GROUP BY objtype
GO

CREATE VIEW qpi.dm_mem_usage
AS
SELECT memory = REPLACE(type, 'MEMORYCLERK_', '') 
     , mem_gb = sum(pages_kb)/1024/1024
	 , mem_perc = ROUND(sum(pages_kb)/1024.0/ (select process_memory_limit_mb - non_sos_mem_gap_mb FROM sys.dm_os_job_object),2)
   FROM sys.dm_os_memory_clerks
   GROUP BY type
   HAVING sum(pages_kb) /1024 /1024 > 0
UNION ALL
	SELECT memory = '_Total',
		mem_gb = (process_memory_limit_mb - non_sos_mem_gap_mb) /1024,
		mem_perc = 1
	FROM sys.dm_os_job_object;
GO
-- https://www.mssqltips.com/sqlservertip/2393/determine-sql-server-memory-use-by-database-and-object/
CREATE VIEW qpi.dm_db_mem_usage
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
[db_name] = CASE [database_id] WHEN 32767 
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

CREATE OR ALTER VIEW
qpi.dm_recommendations
AS
SELECT	name, reason, score, 
		[state] = JSON_VALUE(state, '$.currentValue'),
        script = JSON_VALUE(details, '$.implementationDetails.script'),
        details
FROM sys.dm_db_tuning_recommendations;
GO

---------------------------------------------------------------------------------------------------------
--			High availability
---------------------------------------------------------------------------------------------------------
CREATE OR ALTER VIEW
qpi.nodes
AS
with nodes as (
	select db_name = DB_NAME(database_id), 
		minlsn = CONVERT(NUMERIC(38,0), ISNULL(truncation_lsn, 0)), 
		maxlsn = CONVERT(NUMERIC(38,0), ISNULL(last_hardened_lsn, 0)),
		seeding_state =
			CASE WHEN seedStats.internal_state_desc NOT IN ('Success', 'Failed') OR synchronization_health = 1
						THEN 'Warning' ELSE
                     (CASE WHEN synchronization_state = 0 OR synchronization_health != 2 
							THEN 'ERROR' ELSE 'OK' END)
			END,
		replication_endpoint_url =
		CASE WHEN replication_endpoint_url IS NULL AND (synchronization_state = 1  OR fccs.partner_server IS NOT NULL)
			THEN fccs.partner_server + ' - ' + fccs.partner_database -- Geo replicas will be in Synchronizing state
        ELSE replication_endpoint_url END,
		repl_states.* , seedStats.internal_state_desc, frs.fabric_replica_role
		from sys.dm_hadr_database_replica_states repl_states
              LEFT JOIN sys.dm_hadr_fabric_replica_states frs 
                     ON repl_states.replica_id = frs.replica_id
              LEFT OUTER JOIN sys.dm_hadr_physical_seeding_stats seedStats
                     ON seedStats.remote_machine_name = replication_endpoint_url
                     AND (seedStats.local_database_name = repl_states.group_id OR seedStats.local_database_name = DB_NAME(database_id))
                     AND seedStats.internal_state_desc NOT IN ('Success', 'Failed')
              LEFT OUTER JOIN sys.dm_hadr_fabric_continuous_copy_status fccs
                     ON repl_states.group_database_id = fccs.copy_guid
),
nodes_progress AS (
SELECT *, logprogresssize_p = 
                     CASE WHEN maxlsn - minlsn != 0 THEN maxlsn - minlsn 
                           ELSE 0 END
FROM nodes
),
nodes_progress_size as (
select
	log_progress_size = CASE WHEN last_hardened_lsn > minlsn AND logprogresssize_p > 0
				THEN (CONVERT(NUMERIC(38,0), last_hardened_lsn) - minlsn)*100.0/logprogresssize_p 
    ELSE 0 END,
	*
	from nodes_progress
)
SELECT
	database_id, db_name,
	replication_endpoint_url,
	catchup_progress = CASE WHEN internal_state_desc IS NOT NULL -- Check for active seeding
                           THEN 'Seeding'
                     WHEN logprogresssize_p > 0
                           THEN CONVERT(VARCHAR(100), CONVERT(NUMERIC(20,2),log_progress_size)) + '%' 
                     ELSE 'Select the Primary Node' END,
	is_local,
	is_primary_replica,
	seeding_state,
	synchronization_health_desc,
	synchronization_state_desc,
	secondary_lag_seconds,
	suspend_reason_desc,
	log_send_queue_size,
	log_send_rate,
	redo_queue_size,
	redo_rate,
	recovery_lsn,
	truncation_lsn,
	last_sent_lsn,
	last_received_lsn,
	last_received_time
	last_hardening_lsn,
	last_hardened_time,
	last_redone_lsn,
	last_redone_time,
	end_of_log_lsn,
	last_commit_lsn,
	minlsn, maxlsn
	FROM nodes_progress_size;
GO

CREATE OR ALTER VIEW
qpi.db_nodes
AS
SELECT * FROM qpi.nodes WHERE database_id = DB_ID();
GO