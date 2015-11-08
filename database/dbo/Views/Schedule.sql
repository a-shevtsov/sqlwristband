CREATE VIEW dbo.Schedule
AS
SELECT
	CAST(t.Id*100 + s.MetricGroupId as int) as Id,
	t.Id as TargetId,
	s.MetricGroupId,
	s.OffsetInSecondsFromMidnight,
	s.IntervalInSeconds,
	RetentionPeriodInHours
FROM [dbo].[Targets] t
	CROSS JOIN [dbo].[DefaultSchedule] s
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'View',
	@level1name = N'Schedule',
	@name = N'Revision',
	@value = 1
GO