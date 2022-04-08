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
CREATE OR ALTER VIEW qpi.recommendations
AS
with sql_definition as (
		select 
		object_id,
		format_type = CASE
				WHEN UPPER(m.definition) LIKE '%''PARQUET''%' THEN 'PARQUET'
				WHEN UPPER(m.definition) LIKE '%''DELTA''%' THEN 'DELTA'
				WHEN UPPER(m.definition) LIKE '%''CSV''%' THEN 'CSV'
				WHEN UPPER(m.definition) LIKE '%''COSMOSDB''%' THEN 'COSMOSDB'
				WHEN 		UPPER(m.definition) NOT LIKE '%''PARQUET''%'
						AND (UPPER(m.definition) NOT LIKE '%''CSV''%' )
						AND (UPPER(m.definition) NOT LIKE '%''DELTA''%' )
						AND (UPPER(m.definition) NOT LIKE '%''COSMOSDB''%' )
						THEN 'COMPOSITE'
				ELSE 'MIXED'
			END
		from sys.sql_modules m
),
bulkpath as (
select schema_name = schema_name(v.schema_id), v.name, val =TRIM(SUBSTRING( LOWER(m.definition) , PATINDEX('%bulk%', LOWER(m.definition)), 2048)), m.definition
from sys.views v
join sys.sql_modules m on v.object_id = m.object_id
where PATINDEX('%bulk%', LOWER(m.definition)) > 0
and schema_name(v.schema_id) <> 'qpi'
),
view_path as (
select  name,
		schema_name,
		path = SUBSTRING(val, 
						CHARINDEX('''', val, 0)+1,
						(CHARINDEX('''', val, CHARINDEX('''', val, 0)+1) - CHARINDEX('''', val, 0) - 1)) 
from bulkpath
where CHARINDEX('''', val, 0) > 0
and schema_name <> 'qpi'
),
recommendations as (

select	name = 'USE VARCHAR UTF-8 TYPE',
        score = 1.0,
		schema_name = schema_name(v.schema_id),
		object = v.name,
		column_name = c.name,
		reason =	CONCAT('The view ', v.name, ' that is created on ', m.format_type,' dataset has ', count(c.column_id), ' columns with ') +
					IIF( t.name = 'nchar', 'NVARCHAR/NCHAR type.', 'VARCHAR/CHAR type without UTF-8 collation.') +
					' You might get conversion error.' +
					' Change the column types to VARCHAR with some UTF8 collation.'
from sys.views as v join sys.columns as c on v.object_id = c.object_id
join sql_definition m on v.object_id = m.object_id
join sys.types t on c.user_type_id = t.user_type_id
where (	m.format_type IN ('PARQUET', 'DELTA', 'COSMOSDB', 'MIXED') )
AND	( (t.name iN ('nchar', 'nvarchar')) OR (t.name iN ('nchar', 'nvarchar') AND c.collation_name NOT LIKE '%UTF8') )
group by v.schema_id, v.name, t.name, m.format_type, c.name
union all
-- Tables on UTF-8 files with NVARCHAR/NCHAR columns or CHAR/VARCHAR without UTF8 collation:
select	name = 'USE VARCHAR TYPE',
        score = IIF( t.name LIKE 'n%', 0.3, 1.0),
		schema_name = schema_name(e.schema_id),
		object = e.name,
		column_name = IIF(count(c.column_id)=1, max(c.name), CONCAT(count(c.column_id), ' columns')),
		reason =	CONCAT('The table "', schema_name(e.schema_id), '.', e.name, '" that is created on ', f.format_type, ' files ') +
					CONCAT(IIF( f.encoding = 'UTF8', ' with UTF-8 encoding ', ''), ' has ',
					IIF(count(c.column_id)=1, '"' + max(c.name) + '" column', CONCAT(count(c.column_id), ' columns') ), ' with ') +
					IIF( t.name LIKE 'n%', 'NVARCHAR/NCHAR', 'VARCHAR/CHAR without UTF-8 collation.') +
					' type. Change the column types to VARCHAR with some UTF8 collation.'
from sys.external_tables as e join sys.columns as c on e.object_id = c.object_id
join sys.external_file_formats f on e.file_format_id = f.file_format_id
join sys.types t on c.user_type_id = t.user_type_id
where ( (f.format_type IN ('PARQUET', 'DELTA')) OR f.encoding = 'UTF8' )
AND	( (t.name iN ('nchar', 'nvarchar')) OR (t.name iN ('nchar', 'nvarchar') AND c.collation_name NOT LIKE '%UTF8'))
group by e.schema_id, f.format_type, e.name, f.encoding , t.name, c.name
union all
-- Tables on UTF-16 files with VARCHAR/CHAR columns:
select	name = 'USE NVARCHAR TYPE',
        score = 1.0,
		
		schema_name = schema_name(e.schema_id),
		object = e.name,
		column_name = IIF(count(c.column_id)=1, max(c.name), CONCAT(count(c.column_id), ' columns')),
		reason =	CONCAT('The table "',  schema_name(e.schema_id), '.', e.name, '" created on CSV files with UTF16 encoding has ', 
						IIF(count(c.column_id)=1, '"' + max(c.name) + '" column', CONCAT(count(c.column_id), ' columns') ), ' with ') +
					'VARCHAR/CHAR type. Change the column type to NVARCHAR.'
from sys.external_tables as e join sys.columns as c on e.object_id = c.object_id
join sys.external_file_formats f on e.file_format_id = f.file_format_id
join sys.types t on c.user_type_id = t.user_type_id
where (f.encoding = 'UTF16' )
AND	(t.name iN ('nchar', 'nvarchar'))
group by e.schema_id, f.format_type, e.name, f.encoding , t.name
union all
select	name = 'OPTIMIZE STRING FILTER',
        score = case
					when string_agg(c.name,',') like '%id%' then 0.9
					when string_agg(c.name,',') like '%code%' then 0.9
					when count(c.column_id) > 1 then 0.81
					else 0.71
					end,
		
		schema_name = schema_name(v.schema_id),
		object = v.name,
		column_name = IIF(count(c.column_id)=1, max(c.name), CONCAT(count(c.column_id), ' columns')),
		reason =	CONCAT('The view "',  schema_name(v.schema_id), '.', v.name, '" that is created on ', m.format_type, ' dataset has ',
							IIF(count(c.column_id)=1, '"' + max(c.name) + '" column', CONCAT(count(c.column_id), ' columns') ), ' with ') +
					IIF( t.name = 'nchar', 'NVARCHAR/NCHAR type.', 'VARCHAR/CHAR type without BIN2 UTF8 collation.') +
					' Change the column types to VARCHAR with the Latin1_General_100_BIN2_UTF8 collation.'
from sys.views as v join sys.columns as c on v.object_id = c.object_id
join sql_definition m on v.object_id = m.object_id
join sys.types t on c.user_type_id = t.user_type_id
where (	m.format_type IN ('PARQUET', 'DELTA', 'COSMOSDB', 'MIXED') )
AND	( t.name IN ('char', 'varchar') AND c.collation_name <> 'Latin1_General_100_BIN2_UTF8' )
group by v.schema_id, v.name, t.name, m.format_type

union all

-- Tables on Parquet/Delta Lake files with the columns without BIN2 UTF-8 collation:
select	name = 'OPTIMIZE STRING FILTER',
		score = 0.6,
        schema_name = schema_name(e.schema_id),
		object = e.name,
		column_name = c.name,
		reason = CONCAT('The string column "', c.name, '" in table "', schema_name(t.schema_id), '.', t.name, '" doesn''t have "Latin1_General_100_BIN2_UTF8". String filter on this column are suboptimal')
from sys.external_tables as e join sys.columns as c on e.object_id = c.object_id
join sys.external_file_formats f on e.file_format_id = f.file_format_id
join sys.types t on c.user_type_id = t.user_type_id
where ( (f.format_type IN ('PARQUET', 'DELTA'))) AND t.name IN ('char', 'varchar') AND c.collation_name <> 'Latin1_General_100_BIN2_UTF8'

union all
-- Oversized string columns:
select	name = 'OPTIMIZE COLUMN TYPE',
        score = ROUND(0.3 + (IIF(c.max_length=-1, 0.7*12000., c.max_length)/12000.),1),
		schema_name = schema_name(o.schema_id),
		object = o.name,
		column_name = c.name,
		reason = CONCAT('The string column "', c.name, '" has a max size ', 
				IIF(c.max_length=-1, ' 2 GB', CAST( c.max_length AS VARCHAR(10)) + ' bytes'), '. Check could you use a column with a smaller size.',
				IIF(o.type = 'U', ' Table ', ' View '), '"', schema_name(o.schema_id), '.', o.name, '"')
from sys.objects as o join sys.columns as c on o.object_id = c.object_id
join sys.types t on c.user_type_id = t.user_type_id
where t.name LIKE '%char' AND (c.max_length > 256 OR c.max_length = -1)
and o.type in ('U', 'V')
and lower(c.name) not like '%desc%'
and lower(c.name) not like '%comment%'
and lower(c.name) not like '%note%'
and lower(c.name) not like '%exception%'
and lower(c.name) not like '%reason%'
and lower(c.name) not like '%explanation%'
union all

-- Oversized key columns:
select	name = 'OPTIMIZE KEY COLUMN TYPE',
        score = 0.4 + ROUND((1-EXP(-IIF(c.max_length=-1, 8000., c.max_length)/8000.)),1),
		schema_name = schema_name(o.schema_id),
		object = o.name,
		column_name = c.name,
		reason = CONCAT('Are you using the column "', c.name, '" in join/filter predicates? ',
							'The column type is ', t.name, '(size:',IIF(c.max_length=-1, ' 2 GB', CAST( c.max_length AS VARCHAR(10)) + ' bytes'),'). ',
							'Try to use a column with a smaller type or size.')
from sys.objects as o join sys.columns as c on o.object_id = c.object_id
join sys.types t on c.user_type_id = t.user_type_id
where (c.name LIKE '%code' OR  c.name LIKE '%id') AND (c.max_length > 8 OR c.max_length = -1)
and o.type in ('U', 'V')

union all

-- The tables that are referencing the same location:
select	name = 'REMOVE DUPLICATE REFERENCES',
        score = 0.9,
		schema_name = NULL,
		object = NULL,
		column_name = NULL,
		reason = CONCAT('The tables ', string_agg(concat('"',schema_name(e.schema_id),'.',e.name,'"'), ','), ' are referencing the same location')
from sys.external_tables e
group by data_source_id, location
having count(*) > 1

union all

-- Partitioned external table
select	name = 'REPLACE TABLE WITH PARTITIONED VIEW',
        score = 1.0,
		schema_name = schema_name(e.schema_id),
		object = e.name,
		column_name = NULL,
		reason = CONCAT('The table ', e.name, ' is created on a partitioned data set, but cannot leverage partition elimination. Replace it with a partitioned view.')
from sys.external_tables e
where REPLACE(location, '*.', '') like '%*%'

union all

select	name = 'USE BETTER COLUMN TYPE',
        score = IIF(c.max_length=-1, 1.0, 0.2 + ROUND((1-EXP(-c.max_length/50.))/2,1)),
		schema_name = schema_name(o.schema_id),
		object = o.name,
		column_name = c.name,
		reason = CONCAT('Do you need to use the type "', t.name, '(size:',IIF(c.max_length=-1, ' 2 GB', CAST( c.max_length AS VARCHAR(10)) + ' bytes'),') in column "', c.name, '" in view: "', schema_name(o.schema_id), '.', o.name, '"')
from sys.objects as o join sys.columns as c on o.object_id = c.object_id
join sys.types t on c.user_type_id = t.user_type_id
where
	t.name IN ('nchar', 'nvarchar', 'char', 'varchar', 'binary', 'varbinary')
AND
	(	LOWER(c.name) like '%date%' OR LOWER(c.name) like '%time%' 
	OR	LOWER(c.name) like '%guid%'
	OR	LOWER(c.name) like '%price%' OR LOWER(c.name) like '%amount%' )
AND
	o.type in ('U', 'V')
and lower(c.name) not like '%desc%'
and lower(c.name) not like '%comment%'
and lower(c.name) not like '%note%'
and lower(c.name) not like '%exception%'
and lower(c.name) not like '%reason%'
and lower(c.name) not like '%explanation%'

union all

select	name = 'REMOVE DUPLICATE REFERENCES',
        score = 0.9,
		schema_name = NULL,
		object = NULL,
		column_name = NULL,
		reason = CONCAT('Views ', string_agg(concat(schema_name,'.',name), ','), ' are referencing the same path: ', path)
from view_path
group by path
having count(*) > 1
)
SELECT * FROM recommendations
WHERE schema_name <> 'qpi'
GO
