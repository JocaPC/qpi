--------------------------------------------------------------------------------
--	SQL Server & Azure SQL (Database & Instance) - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------
IF SCHEMA_ID('qpi') IS NULL
	EXEC ('CREATE SCHEMA qpi');
GO

CREATE  FUNCTION qpi.us2min(@microseconds bigint)
RETURNS INT
AS BEGIN RETURN ( @microseconds /1000 /1000 /60 ) END;
GO
-----------------------------------------------------------------------------
--	Part 2. Query store wrappers
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
CREATE
PROCEDURE qpi.clear_db_queries
AS BEGIN
	ALTER DATABASE current SET QUERY_STORE CLEAR;
END
GO
