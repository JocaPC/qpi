SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal
GO

IF SCHEMA_ID('qpi') IS NULL
	EXEC ('CREATE SCHEMA qpi');
GO

CREATE OR ALTER  VIEW qpi.queries
AS

SELECT 	text = substring(text, (statement_start_offset/2)+1,   
								((CASE statement_end_offset  
										WHEN -1 THEN DATALENGTH(text) 
										ELSE statement_end_offset END 
									- statement_start_offset)/2) + 1), 
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
		database_id, connection_id, session_id, request_id = ISNULL(dist_statement_id, CAST(request_id AS VARCHAR(64))), command,
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
        query_text = command,
        data_processed_mb = NULL,
        start_time, end_time,
        transaction_id = NULL,
        status,
        error = NULL, error_code = NULL
FROM [queryinsights].[exec_requests_history]
GO
