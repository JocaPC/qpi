--------------------------------------------------------------------------------
--	SQL Server & Azure SQL Managed Instance - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--	File Statistics
--------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS qpi.snapshot_file_stats;
GO
DROP VIEW IF EXISTS qpi.file_stats;
GO
DROP VIEW IF EXISTS qpi.dm_volumes;
GO
DROP VIEW IF EXISTS qpi.file_stats_snapshots
GO
DROP FUNCTION IF EXISTS qpi.file_stats_as_of;
GO
DROP FUNCTION IF EXISTS qpi.file_stats_at;
GO
BEGIN TRY
	EXEC('ALTER TABLE qpi.dm_io_virtual_file_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;
GO
DROP TABLE IF EXISTS qpi.dm_io_virtual_file_stats_snapshot_history;
GO
DROP TABLE IF EXISTS qpi.dm_io_virtual_file_stats_snapshot;
GO

--------------------------------------------------------------------------------
--	Query info
--------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS qpi.snapshot_wait_stats;
GO
DROP VIEW IF EXISTS qpi.queries;
GO
DROP VIEW IF EXISTS qpi.queries_ex;
GO
DROP VIEW IF EXISTS qpi.dm_queries;
GO
DROP VIEW IF EXISTS qpi.dm_blocked_queries;
GO
DROP VIEW IF EXISTS qpi.query_texts;
GO
DROP VIEW IF EXISTS qpi.query_stats;
GO
DROP VIEW IF EXISTS qpi.query_stats_all;
GO
DROP VIEW IF EXISTS qpi.query_plan_stats;
GO
DROP VIEW IF EXISTS qpi.query_plan_stats_ex;
GO
DROP VIEW IF EXISTS qpi.query_plan_stats_all;
GO
DROP VIEW IF EXISTS qpi.dm_query_stats;
GO
DROP VIEW IF EXISTS qpi.dm_query_locks;
GO
DROP VIEW IF EXISTS qpi.wait_stats
GO
DROP VIEW IF EXISTS qpi.query_plan_wait_stats;
GO
DROP FUNCTION IF EXISTS qpi.wait_stats_as_of;
GO
DROP FUNCTION IF EXISTS qpi.query_stats_as_of;
GO
DROP FUNCTION IF EXISTS qpi.query_plan_stats_as_of;
GO
DROP FUNCTION IF EXISTS qpi.query_plan_stats_ex_as_of;
GO
DROP FUNCTION IF EXISTS qpi.query_plan_wait_stats_as_of;
GO
DROP FUNCTION IF EXISTS qpi.compare_query_stats_on_intervals;
GO
DROP FUNCTION IF EXISTS qpi.query_plan_stats_diff_on_intervals;
GO
DROP FUNCTION IF EXISTS qpi.compare_query_plans;
GO
DROP FUNCTION IF EXISTS qpi.query_wait_stats_as_of;
GO
BEGIN TRY
	EXEC('ALTER TABLE qpi.dm_os_wait_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;
GO
DROP TABLE IF EXISTS qpi.dm_os_wait_stats_snapshot;
GO
DROP TABLE IF EXISTS qpi.dm_os_wait_stats_snapshot_history;
GO

--------------------------------------------------------------------------------
--	Resource info
--------------------------------------------------------------------------------
DROP VIEW IF EXISTS qpi.sys_info;
GO
DROP VIEW IF EXISTS qpi.dm_cpu_usage;
GO
DROP VIEW IF EXISTS qpi.dm_mem_usage;
GO
DROP VIEW IF EXISTS qpi.dm_db_mem_usage;
GO
DROP VIEW IF EXISTS qpi.dm_mem_plan_cache_info;
GO
DROP FUNCTION IF EXISTS qpi.decode_options;
GO
DROP FUNCTION IF EXISTS qpi.ago;
GO
DROP FUNCTION IF EXISTS qpi.dhm;
GO
DROP FUNCTION IF EXISTS qpi.us2min;
GO
DROP FUNCTION IF EXISTS qpi.compare_context_settings;
GO
DROP SCHEMA IF EXISTS qpi;
GO
