--------------------------------------------------------------------------------
--	Synapse serverless SQL pool - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------

SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal
GO

IF SCHEMA_ID('qpi') IS NULL
	EXEC ('CREATE SCHEMA qpi');
GO

CREATE OR ALTER  VIEW qpi.queries
AS
SELECT
		text =   IIF(LEFT(text,1) = '(', TRIM(')' FROM SUBSTRING( text, (PATINDEX( '%)[^),]%', text))+1, LEN(text))), text) ,
		params =  IIF(LEFT(text,1) = '(', SUBSTRING( text, 2, (PATINDEX( '%)[^),]%', text+')'))-2), '') ,
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
		end_time = null,
		sql_handle
FROM    sys.dm_exec_requests
		CROSS APPLY sys.dm_exec_sql_text(sql_handle)
WHERE session_id <> @@SPID

GO

CREATE OR ALTER  VIEW qpi.query_history
AS
SELECT  query_text_id = query_hash,
        request_id = distributed_statement_id,
        elapsed_time_s = total_elapsed_time_ms /1000.,
        query_text = CASE query_text
         WHEN '*** Internal delta query ***' THEN 'Scanning Delta transaction log...'
         WHEN '*** Global stats query ***' THEN 'Collecting file statistics...'
         WHEN '*** External table stats query ***' THEN 'Collecting file statistics...'
         ELSE query_text END,
        data_processed_mb = data_processed_mb,
        start_time, end_time,
        transaction_id,
        status,
        error, error_code
FROM sys.dm_exec_requests_history
GO

CREATE OR ALTER
FUNCTION qpi.cmp_queries (@request_id1 varchar(40), @request_id2 varchar(40))
returns table
return (
	select property = a.[key], a.value query_1, b.value query_2
	from
	(select [key], value
	from openjson(
	(select *
		from qpi.query_history
		where request_id = @request_id1
		for json path, without_array_wrapper)
	)) as a ([key], value)
	join
	(select [key], value
	from openjson(
	(select *
		from qpi.query_history
		where request_id = @request_id2
		for json path, without_array_wrapper)
	)) as b ([key], value)
	on a.[key] = b.[key]
	where a.value <> b.value

);
go
