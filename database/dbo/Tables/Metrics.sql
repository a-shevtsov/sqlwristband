CREATE TABLE [dbo].[Metrics]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1),
	[MetricGroupId] INT NOT NULL CONSTRAINT FK_Metrics_MetricGroups FOREIGN KEY REFERENCES dbo.MetricGroups(Id),
    [Name] NVARCHAR(128) NOT NULL, 
    [DataType] VARCHAR(16) NOT NULL
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'Metrics',
	@name = N'Revision',
	@value = 1
GO