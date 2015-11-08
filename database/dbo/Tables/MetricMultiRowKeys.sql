CREATE TABLE [dbo].[MetricMultiRowKeys]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1),
	[MetricGroupId] INT NOT NULL CONSTRAINT FK_MetricMultiRowKeys_MetricGroups FOREIGN KEY REFERENCES dbo.MetricGroups(Id),
	[IsKeyAttribute] bit NOT NULL,
    [Name] NVARCHAR(128) NOT NULL, 
    [DataType] VARCHAR(16) NOT NULL
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'MetricMultiRowKeys',
	@name = N'Revision',
	@value = 1
GO