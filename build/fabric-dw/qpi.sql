--------------------------------------------------------------------------------
-- Fabric DW - Query Performance Insights
-- Author: Jovan Popovic
--------------------------------------------------------------------------------

SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal
GO

IF SCHEMA_ID('qpi') IS NULL
	EXEC ('CREATE SCHEMA qpi');
GO

-----------------------------------------------------------------------------
-- Generic utilities
-----------------------------------------------------------------------------
CREATE OR ALTER  FUNCTION qpi.label(@sql VARCHAR(max))
RETURNS TABLE
AS RETURN (
    SELECT
        CASE
            WHEN CHARINDEX('(LABEL=', @sql COLLATE  Latin1_General_100_CI_AS_WS_SC_UTF8 ) > 0
                THEN CAST(SUBSTRING(
                    @sql,
                    CHARINDEX('(LABEL=', @sql  COLLATE  Latin1_General_100_CI_AS_WS_SC_UTF8  ) + 8, -- Skip the length of '(LABEL='
                    CHARINDEX("')", @sql, CHARINDEX('(LABEL=', @sql COLLATE  Latin1_General_100_CI_AS_WS_SC_UTF8 ) + 8)
											- CHARINDEX('(LABEL=', @sql  COLLATE  Latin1_General_100_CI_AS_WS_SC_UTF8  ) - 8
                ) AS VARCHAR(8000))
            ELSE NULL
        END AS label
);
GO


CREATE OR ALTER   VIEW qpi.db_query_history
AS
SELECT  query_text_id = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
        request_id = distributed_statement_id,
        duration_s = datediff(second, start_time, end_time),
        text = REPLACE(command, "''","'") ,
        status,
        start_time, end_time,
        interval_id = 	DATEPART(yyyy, (start_time)) * 1000000 +
			DATEPART(mm, (start_time)) * 10000 +
			DATEPART(dd, (start_time)) * 100 +
			DATEPART(hh, (start_time)),
        interval_mi = 60,
        row_count,
        data_processed_mb = NULL,
        query_hash = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
        transaction_id = NULL,
        error = NULL, error_code = NULL,
        label,
        execution_type_desc = status
FROM [queryinsights].[exec_requests_history]
GO
CREATE OR ALTER  VIEW qpi.db_query_stats AS

SELECT
	interval_id =   DATEPART(yyyy, (start_time)) * 1000000 +
			DATEPART(mm, (start_time)) * 10000 +
			DATEPART(dd, (start_time)) * 100 +
			DATEPART(hh, (start_time)),
	text =  REPLACE(command, "''","'") ,
	label = TRIM("'" FROM label),
	status,
	duration_s = CAST(ROUND(AVG(total_elapsed_time_ms/1000.),1) AS DECIMAL(10,1)),
	count_execution = COUNT(*),
	row_count = AVG(row_count),
	interval_mi = 60,
	query_text_id = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	query_hash = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	params = null,
	query_id = null,
	execution_type_desc = status
FROM queryinsights.exec_requests_history
GROUP BY DATEPART(yyyy, start_time)  * 1000000 +
	 DATEPART(mm, start_time) * 10000 +
	 DATEPART(dd, start_time) * 100 +
	 DATEPART(hh, start_time),
	 status, label, command -- Do not use query_hash in Fabric
GO

CREATE OR ALTER  VIEW qpi.db_query_agg_stats
AS

SELECT
	text =  REPLACE(command, "''","'") ,
	label = TRIM("'" FROM label),
	status,
	duration_s = CAST(ROUND(AVG(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	count_execution = COUNT(*),
	row_count = AVG(row_count),
	rows_per_sec =		CAST(ROUND(AVG(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	interval_mi = 60, --MAX(datediff(mi, start_time, end_time)),
	query_text_id = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	query_hash = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	params = null,
	query_id = null,
	execution_type_desc = status
FROM queryinsights.exec_requests_history
GROUP BY status, label, command  -- Do not use query_hash in Fabric
GO


--- Extended statistics views
CREATE OR ALTER VIEW qpi.db_query_stats_ex AS
SELECT
	interval_id = 	DATEPART(yyyy, (start_time)) * 1000000 +
			DATEPART(mm, (start_time)) * 10000 +
			DATEPART(dd, (start_time)) * 100 +
			DATEPART(hh, (start_time)),
	text =  REPLACE(command, "''","'") ,
	label = TRIM("'" FROM label),
	status,
	duration_s = CAST(ROUND(AVG(total_elapsed_time_ms/1000.),1) AS DECIMAL(10,1)),
	duration_min_s = CAST(ROUND(MIN(total_elapsed_time_ms/1000.),1) AS DECIMAL(10,1)),
	duration_max_s = CAST(ROUND(MAX(total_elapsed_time_ms/1000.),1) AS DECIMAL(10,1)),
	duration_dev_s = CAST(ROUND(STDEV(total_elapsed_time_ms/1000.),1) AS DECIMAL(10,1)),
	count_execution = COUNT(*),
	row_count = AVG(row_count),
	rows_per_sec =		CAST(ROUND(AVG(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	rows_per_sec_min =	CAST(ROUND(MIN(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	rows_per_sec_max =	CAST(ROUND(MAX(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	rows_per_sec_stdev = CAST(ROUND(STDEV(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	interval_mi = 60, --MAX(datediff(mi, start_time, end_time)),
	query_text_id = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	query_hash = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	params = null,
	query_id = null,
	session_id = string_agg(cast(session_id as varchar(max)),','),
	request_id = string_agg(cast(distributed_statement_id as varchar(max)),','),
	execution_type_desc = status
FROM queryinsights.exec_requests_history
GROUP BY DATEPART(yyyy, start_time)  * 1000000 +
	 DATEPART(mm, start_time) * 10000 +
	 DATEPART(dd, start_time) * 100 +
	 DATEPART(hh, start_time),
	 status, label, command -- Do not use query_hash in Fabric
GO

CREATE OR ALTER VIEW qpi.db_query_agg_stats_ex AS
SELECT
	text =  REPLACE(command, "''","'") ,
	label = TRIM("'" FROM label),
	status,
	duration_s = CAST(ROUND(AVG(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	duration_min_s = CAST(ROUND(MIN(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	duration_max_s = CAST(ROUND(MAX(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	duration_dev_s = CAST(ROUND(STDEV(total_elapsed_time_ms)/1000.,1) AS DECIMAL(6,1)),
	count_execution = COUNT(*),
	row_count = AVG(row_count),
	rows_per_sec =		CAST(ROUND(AVG(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	rows_per_sec_min =	CAST(ROUND(MIN(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	rows_per_sec_max =	CAST(ROUND(MAX(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	rows_per_sec_stdev = CAST(ROUND(STDEV(row_count/(IIF(total_elapsed_time_ms=0,NULL,total_elapsed_time_ms)/1000.)),1) AS DECIMAL(16,1)),
	interval_mi = 60, --MAX(datediff(mi, start_time, end_time)),
	query_text_id = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	query_hash = CAST(HASHBYTES('MD4', command) AS BIGINT)<<32 + BINARY_CHECKSUM(command),
	params = null,
	query_id = null,
	session_id = string_agg(cast(session_id as varchar(max)),','),
	request_id = string_agg(cast(distributed_statement_id as varchar(max)),','),
	execution_type_desc = status
FROM queryinsights.exec_requests_history
GROUP BY status, label, command  -- Do not use query_hash in Fabric
GO
-----------------------------------------------------------------------------
-- Core Server-level functionalities
-----------------------------------------------------------------------------
-- The list of currently executing queries that are probably not in Query Store.
CREATE OR ALTER  VIEW qpi.queries
AS
SELECT
		text =   substring(text, (statement_start_offset/2)+1, ((CASE statement_end_offset WHEN -1 THEN DATALENGTH(text) ELSE statement_end_offset END - statement_start_offset)/2) + 1) ,
		params =  substring(text, 1, (statement_start_offset/2)) ,
		status,
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
		query_text_id = CAST(HASHBYTES('MD4', text) AS BIGINT)<<32 + BINARY_CHECKSUM(text),
		query_hash = CAST(HASHBYTES('MD4', text) AS BIGINT)<<32 + BINARY_CHECKSUM(text),
		query_id = NULL, plan_id = NULL, database_id, connection_id, session_id,
		request_id = ISNULL(dist_statement_id, CAST(request_id AS VARCHAR(64))), command,
		start_time,
		end_time = null,
		interval_id = DATEPART(yyyy, (start_time)) * 1000000 +
				DATEPART(mm, (start_time)) * 10000 +
				DATEPART(dd, (start_time)) * 100 +
				DATEPART(hh, (start_time)),
		interval_mi = 60,
		sql_handle,
		label,
		execution_type_desc = status
FROM    sys.dm_exec_requests
		CROSS APPLY sys.dm_exec_sql_text(sql_handle)
WHERE session_id <> @@SPID
GO
-----------------------------------------------------------------------------
-- Table statistics
-----------------------------------------------------------------------------

CREATE OR ALTER  VIEW qpi.db_table_stats AS
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

CREATE OR ALTER  VIEW qpi.db_table_stat_columns AS
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
