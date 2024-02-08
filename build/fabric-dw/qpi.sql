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
        elapsed_time_s = datediff(second, start_time, end_time),
        query_text = command,
        status,
        start_time, end_time,
        data_processed_mb = NULL,
        transaction_id = NULL,
        error = NULL, error_code = NULL
FROM [queryinsights].[exec_requests_history]
GO
CREATE OR ALTER VIEW qpi.query_stats AS
SELECT
	text = MAX(command),
	execution_type_desc = status,
	duration_s = ROUND(AVG(total_elapsed_time_ms)/1000,1),
	duration_min_s = ROUND(MIN(total_elapsed_time_ms)/1000,1),
	duration_max_s = ROUND(AVG(total_elapsed_time_ms)/1000,1),
	duration_dev_s = ROUND(AVG(total_elapsed_time_ms)/1000,1),
	count_execution = COUNT(*),
	row_count = AVG(row_count),
	start_time = MIN(start_time),
	end_time = MAX(end_time),
	interval_mi = MAX(datediff(mi, start_time, end_time)),
	query_text_id = query_hash,
	query_hash = query_hash,
	params = null,
	query_id = null,
	session_id = max(session_id),
	request_id = max(distributed_statement_id)
FROM queryinsights.exec_requests_history
GROUP BY query_hash, status
GO
CREATE OR ALTER FUNCTION qpi.label (@sql_text varchar(max))
RETURNS TABLE
AS RETURN ( SELECT label = SUBSTRING(@sql_text,
						PATINDEX('%/*%',@sql_text)+2, 
							IIF((PATINDEX('%*/%',@sql_text)-PATINDEX('%/*%',@sql_text)-2)>0,
								(PATINDEX('%*/%',@sql_text)-PATINDEX('%/*%',@sql_text)-2), 0))
								)
GO
