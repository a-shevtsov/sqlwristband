CREATE PROCEDURE [dbo].[DeleteTarget]
	@TargetId int
AS
BEGIN
	DECLARE
		@schemaName sysname,
		@objectName sysname,
		@dropStatement nvarchar(max)

	SELECT @schemaName = dbo.GetTargetSchemaName(@TargetId)

	-- Remove archive watermarks
	DELETE FROM dbo.ArchiveWatermarks WHERE TargetId = @TargetId;

	-- Delete target itself
	DELETE FROM dbo.Targets WHERE Id = @TargetId

	-- Drop data tables
	DECLARE dataTablesCur CURSOR
		FOR SELECT name FROM sys.tables
			WHERE schema_id = SCHEMA_ID(@schemaName) AND name LIKE N'%[_]data[_]%';

	OPEN dataTablesCur;

	FETCH NEXT FROM dataTablesCur INTO @objectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC(N'DROP TABLE [' + @schemaName + N'].[' + @objectName + N']');
		FETCH NEXT FROM dataTablesCur INTO @objectName;
	END

	CLOSE dataTablesCur;
	DEALLOCATE dataTablesCur;

	-- Drop dictionaries
	DECLARE dictTablesCur CURSOR
		FOR SELECT name FROM sys.tables
			WHERE schema_id = SCHEMA_ID(@schemaName) AND name LIKE N'%[_]dict[_]%'

	OPEN dictTablesCur;

	FETCH NEXT FROM dictTablesCur INTO @objectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC(N'DROP TABLE [' + @schemaName + N'].[' + @objectName + N']');
		FETCH NEXT FROM dictTablesCur INTO @objectName;
	END

	CLOSE dictTablesCur;
	DEALLOCATE dictTablesCur;

	-- Drop sequence objects
	DECLARE sequenceCur CURSOR
		FOR SELECT name FROM sys.sequences
			WHERE schema_id = SCHEMA_ID(@schemaName)

	OPEN sequenceCur;

	FETCH NEXT FROM sequenceCur INTO @objectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC(N'DROP SEQUENCE [' + @schemaName + N'].[' + @objectName + N']');
		FETCH NEXT FROM sequenceCur INTO @objectName;
	END

	CLOSE sequenceCur;
	DEALLOCATE sequenceCur;

	-- Drop schema
	IF EXISTS (SELECT NULL FROM sys.schemas WHERE name = @schemaName)
		EXEC(N'DROP SCHEMA [' + @schemaName + N']');

	RETURN 0
END
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Procedure',
	@level1name = N'DeleteTarget',
	@name = N'Revision',
	@value = 1
GO