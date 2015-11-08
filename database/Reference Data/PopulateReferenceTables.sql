DECLARE @metricGroupId int

INSERT INTO [dbo].[MetricGroups]
	(Name, ProbeCode, ChangeSpeed, IsMultiRow, IsCumulative, MultiRowKeyAttributesChangeSpeed)
VALUES
	('SQL Server Configuration', 'SqlServer', 'Slow', 'FALSE', 'FALSE', NULL);

SELECT @metricGroupId = Id
FROM [dbo].[MetricGroups]
WHERE Name = 'SQL Server Configuration';

SET QUOTED_IDENTIFIER OFF;

UPDATE [dbo].[MetricGroups]
SET Script = "SELECT
    CAST(SERVERPROPERTY('Edition') as varchar(128)) as [Edition],
	CAST(SERVERPROPERTY('ProductVersion') as varchar(128)) as [Version],
	CAST(SERVERPROPERTY('ProductLevel') as varchar(128)) as [Level],
	(SELECT CAST(value_in_use as real) FROM sys.configurations WHERE name = 'max server memory (MB)') as [Max_Server_Memory],
	(SELECT CAST(COUNT(*) as smallint) FROM sys.databases WHERE name NOT IN ('master','model','tempdb','msdb')) as [Number_of_User_Databases]"
WHERE Id = @metricGroupId;

SET QUOTED_IDENTIFIER ON;

INSERT INTO [dbo].[Metrics]
(MetricGroupId, Name, DataType)
VALUES
	(@metricGroupId, 'Edition', 'Ansi'),
	(@metricGroupId, 'Version', 'Ansi'),
	(@metricGroupId, 'Level', 'Ansi'),
	(@metricGroupId, 'Max Server Memory', 'Real'),
	(@metricGroupId, 'Number of User Databases', 'SmallInt');


GO

DECLARE @metricGroupId int

INSERT INTO [dbo].[MetricGroups]
	(Name, ProbeCode, ChangeSpeed, IsMultiRow, IsCumulative, MultiRowKeyAttributesChangeSpeed)
VALUES
	('SQL Server Wait Stats', 'SqlServer', 'Fast', 'TRUE', 'TRUE', 'Static');

SELECT @metricGroupId = Id
FROM [dbo].[MetricGroups]
WHERE Name = 'SQL Server Wait Stats';

UPDATE [dbo].[MetricGroups]
SET Script = 'SELECT
    wait_type,
    CAST(waiting_tasks_count as real) as [Waiting_Tasks_Count],
    CAST(wait_time_ms as real) as [Wait_Time_ms],
    CAST(signal_wait_time_ms as real) as [Signal_Wait_Time_ms]
FROM sys.dm_os_wait_stats
WHERE waiting_tasks_count > 0'
WHERE Id = @metricGroupId;

INSERT INTO [dbo].[MetricMultiRowKeys]
(MetricGroupId, IsKeyAttribute, Name, DataType)
VALUES
	(@metricGroupId, 'FALSE', 'Wait Type', 'Ansi');

INSERT INTO [dbo].[Metrics]
(MetricGroupId, Name, DataType)
VALUES
	(@metricGroupId, 'Waiting Tasks Count', 'Real'),
	(@metricGroupId, 'Wait Time ms', 'Real'),
	(@metricGroupId, 'Signal Wait Time ms', 'Real');

GO

DECLARE @metricGroupId int

INSERT INTO [dbo].[MetricGroups]
(Name, ProbeCode, ChangeSpeed, IsMultiRow, IsCumulative, MultiRowKeyAttributesChangeSpeed)
VALUES
	('SQL Server File Stats', 'SqlServer', 'Fast', 'TRUE', 'TRUE', 'Slow');

SELECT @metricGroupId = Id
FROM [dbo].[MetricGroups]
WHERE Name = 'SQL Server File Stats';

UPDATE [dbo].[MetricGroups]
SET Script = 'SELECT
    COALESCE(CONVERT(nvarchar(36), mf.file_guid), mf.name) as [FileGuid],
    DB_NAME(vfs.database_id) as [Database_Name],
	mf.name as [Logical_File_Name],
    mf.physical_name as [Physical_File_Name],
    CAST(mf.size as real) as [File_Size],
	CAST(vfs.num_of_reads as real) as [Number_of_reads],
    CAST(vfs.num_of_bytes_read as real) as [Number_of_bytes_read],
    CAST(vfs.num_of_writes as real) as [Number_of_writes],
    CAST(vfs.num_of_bytes_written as real) as [Number_of_bytes_written],
    CAST(vfs.io_stall_read_ms as real) as [IO_stall_read_ms],
    CAST(vfs.io_stall_write_ms as real) as [IO_stall_write_ms]
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
	INNER JOIN master.sys.master_files mf
		ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id'
WHERE Id = @metricGroupId;

INSERT INTO [dbo].[MetricMultiRowKeys]
(MetricGroupId, IsKeyAttribute, Name, DataType)
VALUES
    (@metricGroupId, 'FALSE', 'FileGuid', 'Ansi'),
    (@metricGroupId, 'FALSE', 'Database Name', 'Unicode'),
    (@metricGroupId, 'TRUE', 'Logical File Name', 'Unicode'),
    (@metricGroupId, 'TRUE', 'Physical File Name', 'Unicode'),
    (@metricGroupId, 'TRUE', 'File Size', 'Real');

INSERT INTO [dbo].[Metrics]
(MetricGroupId, Name, DataType)
VALUES
	(@metricGroupId, 'Number of reads', 'Real'),
	(@metricGroupId, 'Number of bytes read', 'Real'),
	(@metricGroupId, 'Number of writes', 'Real'),
	(@metricGroupId, 'Number of bytes written', 'Real'),
	(@metricGroupId, 'IO stall read ms', 'Real'),
	(@metricGroupId, 'IO stall write ms', 'Real');

GO

DECLARE @metricGroupId int

INSERT INTO [dbo].[MetricGroups]
(Name, ProbeCode, ChangeSpeed, IsMultiRow, IsCumulative, MultiRowKeyAttributesChangeSpeed)
VALUES
	('SQL Server Physical Memory Stats', 'SqlServer', 'Fast', 'FALSE', 'FALSE', NULL);

SELECT @metricGroupId = Id
FROM [dbo].[MetricGroups]
WHERE Name = 'SQL Server Physical Memory Stats';

UPDATE [dbo].[MetricGroups]
SET Script = 'SELECT CAST(physical_memory_kb as real) as [Physical_Memory_KB] FROM sys.dm_os_sys_info'
WHERE Id = @metricGroupId;

INSERT INTO [dbo].[Metrics]
(MetricGroupId, Name, DataType)
VALUES
	(@metricGroupId, 'Physical Memory KB', 'Real');

GO

DECLARE @metricGroupId int

INSERT INTO [dbo].[MetricGroups]
(Name, ProbeCode, ChangeSpeed, IsMultiRow, IsCumulative, MultiRowKeyAttributesChangeSpeed)
VALUES
	('SQL Server Activity', 'SqlServer', 'Fast', 'FALSE', 'TRUE', NULL);

SELECT @metricGroupId = Id
FROM [dbo].[MetricGroups]
WHERE Name = 'SQL Server Activity';

UPDATE [dbo].[MetricGroups]
SET Script = 'SELECT
    CAST(@@CPU_BUSY*@@TIMETICKS/1000 as real) as [CPU_mils],
    CAST(@@TOTAL_READ as real) as [Physical_Reads],
    CAST(@@TOTAL_WRITE as real) as [Physical_Writes]'
WHERE Id = @metricGroupId;

INSERT INTO [dbo].[Metrics]
(MetricGroupId, Name, DataType)
VALUES
	(@metricGroupId, 'CPU mils', 'Real'),
	(@metricGroupId, 'Physical Reads', 'Real'),
	(@metricGroupId, 'Physical Writes', 'Real');

GO
