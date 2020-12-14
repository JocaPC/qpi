--------------------------------------------------------------------------------
--	SQL Server & Azure SQL Managed Instance - Query Performance Insights
--	Author: Jovan Popovic
--------------------------------------------------------------------------------
DECLARE @name VARCHAR(128)
DECLARE @SQL VARCHAR(MAX)

SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'P' AND SCHEMA_NAME(schema_id) = 'qpi'  ORDER BY [name])

WHILE @name is not null
BEGIN
    SELECT @SQL = 'DROP PROCEDURE [qpi].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Procedure: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'P' AND SCHEMA_NAME(schema_id) = 'qpi'  AND [name] > @name ORDER BY [name])
END

/* Drop all views */

SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'V' AND SCHEMA_NAME(schema_id) = 'qpi'  ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP VIEW [qpi].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped View: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'V' AND SCHEMA_NAME(schema_id) = 'qpi'  AND [name] > @name ORDER BY [name])
END

/* Drop all functions */
SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND SCHEMA_NAME(schema_id) = 'qpi'  ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP FUNCTION [qpi].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Function: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND SCHEMA_NAME(schema_id) = 'qpi'  AND [name] > @name ORDER BY [name])
END

/* Drop all tables */

BEGIN TRY
	EXEC('ALTER TABLE qpi.io_virtual_file_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;

BEGIN TRY
	EXEC('ALTER TABLE qpi.dm_io_virtual_file_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;

BEGIN TRY
	EXEC('ALTER TABLE qpi.os_performance_counters_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;

BEGIN TRY
	EXEC('ALTER TABLE qpi.dm_os_performance_counters_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;

BEGIN TRY
	EXEC('ALTER TABLE qpi.os_wait_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;

BEGIN TRY
	EXEC('ALTER TABLE qpi.dm_os_wait_stats_snapshot 
			SET (SYSTEM_VERSIONING = OFF)');
END TRY BEGIN CATCH END CATCH;

SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'U'  AND SCHEMA_NAME(schema_id) = 'qpi' ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP TABLE [qpi].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Table: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sys.objects WHERE [type] = 'U'  AND SCHEMA_NAME(schema_id) = 'qpi'  AND [name] > @name ORDER BY [name])
END
GO
DROP SCHEMA qpi;
