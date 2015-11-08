CREATE TABLE [dbo].[MetricGroups]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1), 
    [Name] NVARCHAR(128) NOT NULL, 
    [ProbeCode] VARCHAR(64) NOT NULL, 
    [ChangeSpeed] VARCHAR(16) NOT NULL, 
    [IsMultiRow] BIT NOT NULL, 
    [IsCumulative] BIT NOT NULL, 
    [MultiRowKeyAttributesChangeSpeed] VARCHAR(16) NULL, 
    [Script] NVARCHAR(MAX) NULL
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'MetricGroups',
	@name = N'Revision',
	@value = 1
GO