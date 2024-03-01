
SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal
GO

IF SCHEMA_ID('qpi') IS NULL
	EXEC ('CREATE SCHEMA qpi');
GO

CREATE OR ALTER FUNCTION qpi.label(@sql VARCHAR(max))
RETURNS TABLE
AS RETURN (
    SELECT
        CASE
            WHEN CHARINDEX('(LABEL=', @sql) > 0
                THEN SUBSTRING(
                    @sql,
                    CHARINDEX('(LABEL=', @sql) + 8, -- Skip the length of '(LABEL='
                    CHARINDEX(''')', @sql, CHARINDEX('(LABEL=', @sql) + 8) - CHARINDEX('(LABEL=', @sql) - 8
                )
            ELSE 'N/A'
        END AS label
);
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
        text = REPLACE(command, '''''',''''),
        status,
        start_time, end_time,
        data_processed_mb = NULL,
        transaction_id = NULL,
        error = NULL, error_code = NULL
FROM [queryinsights].[exec_requests_history]
GO
CREATE OR ALTER VIEW qpi.query_stats AS
SELECT
	text = MAX(REPLACE(command, '''''','''')),
	execution_type_desc = status,
	duration_s = CAST(ROUND(AVG(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	duration_min_s = CAST(ROUND(MIN(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	duration_max_s = CAST(ROUND(MAX(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	duration_dev_s = CAST(ROUND(STDEV(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	count_execution = COUNT(*),
	row_count = AVG(row_count),
	rows_per_sec =		CAST(ROUND(AVG(row_count/(total_elapsed_time_ms/1000.)),1) AS DECIMAL(6,1)),
	rows_per_sec_min =	CAST(ROUND(MIN(row_count/(total_elapsed_time_ms/1000.)),1) AS DECIMAL(6,1)),
	rows_per_sec_max =	CAST(ROUND(MAX(row_count/(total_elapsed_time_ms/1000.)),1) AS DECIMAL(6,1)),
	start_time = MIN(start_time),
	end_time = MAX(end_time),
	interval_mi = MAX(datediff(mi, start_time, end_time)),
	query_text_id = query_hash,
	query_hash = query_hash,
	params = null,
	query_id = null,
	session_id = string_agg(cast(session_id as varchar(max)),','),
	request_id = string_agg(cast(distributed_statement_id as varchar(max)),',')
FROM queryinsights.exec_requests_history
where total_elapsed_time_ms > 0
GROUP BY query_hash, status
GO

CREATE OR ALTER VIEW qpi.table_stats AS
SELECT
	s.name,
	table_name = OBJECT_NAME(s.object_id), 
	sc.columns,
	stats_method = s.stats_generation_method_desc,
	auto_created,
	auto_drop,
	user_created,
	is_incremental,
	filter = IIF(has_filter=1, filter_definition, 'N/A')
FROM sys.stats s
	JOIN (SELECT	sc.stats_id, sc.object_id,
					columns = STRING_AGG(cast(c.name as varchar(max)), ',')
			FROM sys.stats_columns sc, sys.columns c
			WHERE sc.object_id = c.object_id
			AND sc.column_id = c.column_id
			AND OBJECTPROPERTY(c.object_id, 'IsMSShipped') = 0 
			GROUP BY sc.stats_id, sc.object_id
			) sc ON s.stats_id = sc.stats_id AND s.object_id = sc.object_id
WHERE OBJECTPROPERTY(s.object_id, 'IsMSShipped') = 0
GO

CREATE OR ALTER VIEW qpi.table_stat_columns AS
SELECT	name = s.name,
	table_name = OBJECT_NAME(s.object_id), 
	column_name = c.name, 
	type = CASE 
		WHEN t.name IN ('decimal', 'numeric') THEN CONCAT(t.name, '(', c.precision, ',', c.scale, ')')
		WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary', 'datetime2', 'time', 'datetimeoffset') THEN CONCAT(t.name, '(', IIF(c.max_length <> -1, CAST(c.max_length AS VARCHAR(100)), 'MAX'), ')')
		ELSE t.name
	END,
	stats_method = s.stats_generation_method_desc,
	auto_created,
	auto_drop,
	user_created,
	is_incremental,
	filter = IIF(has_filter=1, filter_definition, 'N/A')
FROM sys.stats s,sys.stats_columns sc, sys.columns c, sys.types t, sys.objects o
WHERE s.object_id = sc.object_id AND s.stats_id = sc.stats_id
AND sc.column_id = c.column_id
AND s.object_id = c.object_id
AND o.object_id = c.object_id
AND c.system_type_id = t.system_type_id
AND OBJECTPROPERTY(s.object_id, 'IsMSShipped') = 0
