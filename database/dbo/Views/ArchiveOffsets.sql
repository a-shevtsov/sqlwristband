CREATE VIEW [dbo].[ArchiveOffsets]
AS
SELECT
	s.Id*20 + a.[Id] as Id,
	s.[Id] as ScheduleId,
	s.TargetId,
	a.[OffsetInMinutes],
	a.[IntervalInSeconds]
FROM dbo.Schedule s
	INNER JOIN [dbo].[DefaultArchiveOffsets] a
		ON s.MetricGroupId = a.MetricGroupId
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'View',
	@level1name = N'ArchiveOffsets',
	@name = N'Revision',
	@value = 1
GO