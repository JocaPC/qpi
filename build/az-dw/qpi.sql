--------------------------------------------------------------------------------
--	SQL Server & Azure SQL (Database & Instance) - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------
IF SCHEMA_ID('qpi') IS NULL
	EXEC ('CREATE SCHEMA qpi');
GO

-----------------------------------------------------------------------------
-- Generic utilities
-----------------------------------------------------------------------------
CREATE  FUNCTION qpi.us2min(@microseconds bigint)
RETURNS INT
AS BEGIN RETURN ( @microseconds /1000 /1000 /60 ) END;
GO
-----------------------------------------------------------------------------
-- Core Database Query Store functionalities
-----------------------------------------------------------------------------
CREATE  FUNCTION qpi.decode_options(@options int)
RETURNS TABLE
RETURN (
SELECT 'DISABLE_DEF_CNST_CHK'  = (1 & @options) / 1
	, 'IMPLICIT_TRANSACTIONS' = (2 & @options) / 2
	, 'CURSOR_CLOSE_ON_COMMIT' = (4 & @options) / 4
	, 'ANSI_WARNINGS' = (8 & @options) / 8
	, 'ANSI_PADDING' = (16 & @options) / 16
	, 'ANSI_NULLS' = (32 & @options) / 32
	, 'ARITHABORT' = (64 & @options) / 64
	, 'ARITHIGNORE' = (128 & @options) / 128
	, 'QUOTED_IDENTIFIER' = (256 & @options) / 256
	, 'NOCOUNT' = (512 & @options) / 512
	, 'ANSI_NULL_DFLT_ON' = (1024 & @options) / 1024
	, 'ANSI_NULL_DFLT_OFF' = (2048 & @options) / 2048
	, 'CONCAT_NULL_YIELDS_NULL' = (4096 & @options) / 4096
	, 'NUMERIC_ROUNDABORT' = (8192 & @options) / 8192
	, 'XACT_ABORT' = (16384 & @options) / 16384
)
GO
GO
CREATE  VIEW qpi.db_queries
as
select	text =  CASE LEFT(query_sql_text,1) WHEN '(' THEN SUBSTRING( query_sql_text, (PATINDEX( '%)[^),]%', query_sql_text+')'))+1, LEN(query_sql_text)) ELSE query_sql_text END ,
		params =  CASE LEFT(query_sql_text,1) WHEN '(' THEN SUBSTRING( query_sql_text, 2, (PATINDEX( '%)[^),]%', query_sql_text+')'))-2) ELSE 'N/A' END ,
		q.query_text_id, query_id, context_settings_id, q.query_hash
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
GO

CREATE  VIEW qpi.db_queries_ex
as
select	q.text, q.params, q.query_text_id, query_id, q.context_settings_id, q.query_hash,
		o.*
FROM qpi.db_queries q
		JOIN sys.query_context_settings ctx
			ON q.context_settings_id = ctx.context_settings_id
			CROSS APPLY qpi.decode_options(ctx.set_options) o
GO

CREATE  VIEW qpi.db_query_texts
as
select	q.text, q.params, q.query_text_id, queries =  count(query_id)
from qpi.db_queries q
group by q.text, q.params, q.query_text_id
GO

CREATE  VIEW qpi.db_query_plans
as
select	q.text, q.params, q.query_text_id, p.plan_id, p.query_id,
		p.compatibility_level, p.query_plan_hash, p.count_compiles,
		p.is_parallel_plan, p.is_forced_plan, p.query_plan, q.query_hash
from sys.query_store_plan p
	join qpi.db_queries q
		on p.query_id = q.query_id;
GO

CREATE  VIEW qpi.db_query_plans_ex
as
select	q.text, q.params, q.query_text_id, p.*, q.query_hash
from sys.query_store_plan p
	join qpi.db_queries q
		on p.query_id = q.query_id;
GO
-----------------------------------------------------------------------------
-- Advanced Database Query Store functionalities
-----------------------------------------------------------------------------

CREATE
FUNCTION qpi.db_query_plan_exec_stats_as_of(@date datetime2)
returns table
as return (
select	t.query_text_id, q.query_id,
		text =   CASE LEFT(t.query_sql_text,1) WHEN '(' THEN SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^),]%', t.query_sql_text+')'))+1, LEN(t.query_sql_text)) ELSE t.query_sql_text END ,
		params =  CASE LEFT(t.query_sql_text,1) WHEN '(' THEN SUBSTRING( t.query_sql_text, 2, (PATINDEX( '%)[^),]%', t.query_sql_text+')'))-2) ELSE 'N/A' END ,
		rs.plan_id,
		rs.execution_type_desc,
        rs.count_executions,
        duration_s = CAST(ROUND( rs.avg_duration /1000.0 /1000.0, 2) AS NUMERIC(12,2)),
        cpu_time_ms = CAST(ROUND(rs.avg_cpu_time /1000.0, 1) AS NUMERIC(12,1)),
        logical_io_reads_kb = CAST(ROUND(rs.avg_logical_io_reads * 8 /1000.0, 2) AS NUMERIC(12,2)),
        logical_io_writes_kb = CAST(ROUND(rs.avg_logical_io_writes * 8 /1000.0, 2) AS NUMERIC(12,2)),
        physical_io_reads_kb = CAST(ROUND(rs.avg_physical_io_reads * 8 /1000.0, 2) AS NUMERIC(12,2)),
        clr_time_ms = CAST(ROUND(rs.avg_clr_time /1000.0, 1) AS NUMERIC(12,1)),
        max_used_memory_mb = rs.avg_query_max_used_memory * 8.0 /1000,
		start_time = convert(varchar(16), rsi.start_time, 20),
		end_time = convert(varchar(16), rsi.end_time, 20),
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time),
		q.context_settings_id, q.query_hash
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi
			on rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where (@date is null or @date between rsi.start_time and rsi.end_time)
);
GO

CREATE
VIEW qpi.db_query_plan_exec_stats
AS SELECT * FROM qpi.db_query_plan_exec_stats_as_of(GETUTCDATE());
GO

CREATE
VIEW qpi.db_query_plan_exec_stats_history
AS SELECT * FROM qpi.db_query_plan_exec_stats_as_of(NULL);
GO

-- Returns all query plan statistics without currently running values.
CREATE
FUNCTION qpi.db_query_plan_exec_stats_ex_as_of(@date datetime2)
returns table
as return (
select	q.query_id,
		text =   CASE LEFT(t.query_sql_text,1) WHEN '(' THEN SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^),]%', t.query_sql_text+')'))+1, LEN(t.query_sql_text)) ELSE t.query_sql_text END ,
		params =  CASE LEFT(t.query_sql_text,1) WHEN '(' THEN SUBSTRING( t.query_sql_text, 2, (PATINDEX( '%)[^),]%', t.query_sql_text+')'))-2) ELSE 'N/A' END ,
		t.query_text_id, rsi.start_time, rsi.end_time,
		rs.*, q.query_hash,
		interval_mi = datediff(mi, rsi.start_time, rsi.end_time)
from sys.query_store_query_text t
	join sys.query_store_query q on t.query_text_id = q.query_text_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval rsi on rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
where @date is null or @date between rsi.start_time and rsi.end_time
);
GO

CREATE
VIEW qpi.db_query_plan_exec_stats_ex
AS SELECT * FROM qpi.db_query_plan_exec_stats_ex_as_of(GETUTCDATE());
GO
--------------------------------------------------------------------------------
-- the most important view: query statistics:
--------------------------------------------------------------------------------
-- Returns statistics about all queries as of specified time.
CREATE  FUNCTION qpi.db_query_exec_stats_as_of(@date datetime2)
returns table
return (

WITH query_stats as (
SELECT	qps.query_id, execution_type_desc,
		duration_s = AVG(duration_s),
		count_executions = SUM(count_executions),
		cpu_time_ms = AVG(cpu_time_ms),
		logical_io_reads_kb = AVG(logical_io_reads_kb),
		logical_io_writes_kb = AVG(logical_io_writes_kb),
		physical_io_reads_kb = AVG(physical_io_reads_kb),
		clr_time_ms = AVG(clr_time_ms),
		start_time = MIN(start_time),
		interval_mi = MIN(interval_mi)
FROM qpi.db_query_plan_exec_stats_as_of(@date) qps
GROUP BY query_id, execution_type_desc
)
SELECT  text =   CASE LEFT(t.query_sql_text,1) WHEN '(' THEN SUBSTRING( t.query_sql_text, (PATINDEX( '%)[^),]%', t.query_sql_text+')'))+1, LEN(t.query_sql_text)) ELSE t.query_sql_text END ,
		params =  CASE LEFT(t.query_sql_text,1) WHEN '(' THEN SUBSTRING( t.query_sql_text, 2, (PATINDEX( '%)[^),]%', t.query_sql_text+')'))-2) ELSE 'N/A' END ,
		qs.*,
		t.query_text_id,
		q.query_hash
FROM query_stats qs
	join sys.query_store_query q
	on q.query_id = qs.query_id
	join sys.query_store_query_text t
	on q.query_text_id = t.query_text_id

)
GO

CREATE  VIEW qpi.db_query_exec_stats
AS SELECT * FROM  qpi.db_query_exec_stats_as_of(GETUTCDATE());
GO
CREATE  VIEW qpi.db_query_exec_stats_history
AS SELECT * FROM  qpi.db_query_exec_stats_as_of(NULL);
GO

CREATE  VIEW qpi.db_query_stats
AS
SELECT text, params, qes.execution_type_desc, qes.query_id, count_executions, duration_s, cpu_time_ms,
 logical_io_reads_kb, logical_io_writes_kb, physical_io_reads_kb, clr_time_ms, qes.start_time, qes.query_hash
FROM qpi.db_query_exec_stats qes
GO

CREATE  VIEW
qpi.db_query_stats_history
AS
SELECT text, params, qes.execution_type_desc, qes.query_id, count_executions, duration_s, cpu_time_ms,
 logical_io_reads_kb, logical_io_writes_kb, physical_io_reads_kb, clr_time_ms, qes.start_time, qes.query_hash
FROM qpi.db_query_exec_stats_history qes
GO
CREATE  VIEW qpi.queries
AS
SELECT
        text = command,
        params = NULL,
		execution_type_desc = status COLLATE Latin1_General_CS_AS,
		first_execution_time = start_time, last_execution_time = NULL, count_executions = NULL,
		elapsed_time_s = total_elapsed_time /1000.0,
		cpu_time_s = NULL, -- N/A in DW
		logical_io_reads = NULL,
		logical_io_writes = NULL,
		physical_io_reads = NULL,
		num_physical_io_reads = NULL,
		clr_time = NULL,
		dop = NULL,
		row_count = NULL,
		memory_mb = NULL,
		log_bytes = NULL,
		tempdb_space = NULL,
		query_text_id = NULL, query_id = NULL, plan_id = NULL,
		database_id,
        connection_id = client_correlation_id,
        session_id, request_id, command,
		interval_mi = null,
		start_time,
		end_time = null,
		sql_handle = NULL
FROM    sys.dm_pdw_exec_requests
WHERE command NOT LIKE '%qpi.queries%'
  AND status NOT IN ('Completed', 'Failed')
GO
