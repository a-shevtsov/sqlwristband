CREATE TABLE [dbo].[ArchiveWatermarks]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1),
	[TargetId] INT NOT NULL CONSTRAINT FK_ArchiveWatermarks_Targets FOREIGN KEY REFERENCES dbo.Targets (Id),
    [ArchiveOffsetId] INT NOT NULL,
    [ArchivedToDate] DATETIME2(0) NOT NULL,
	CONSTRAINT UIX_ArchiveWatermarks_TargetId_ArchiveOffsetId UNIQUE NONCLUSTERED
	(
		[TargetId],
		[ArchiveOffsetId]
	)
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'ArchiveWatermarks',
	@name = N'Revision',
	@value = 1
GO