CREATE TABLE [dbo].[DefaultArchiveOffsets]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1), 
    [MetricGroupId] INT NOT NULL CONSTRAINT FK_DefaultArchiveOffsets_MetricGroups FOREIGN KEY REFERENCES dbo.MetricGroups (Id),
    [OffsetInMinutes] INT NOT NULL,
	[IntervalInSeconds] INT NOT NULL,
	CONSTRAINT UIX_DefaultArchiveOffsets_MetricGroupId_OffsetInMinutes UNIQUE NONCLUSTERED
	(
		[MetricGroupId],
		[OffsetInMinutes]
	)
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'DefaultArchiveOffsets',
	@name = N'Revision',
	@value = 1
GO