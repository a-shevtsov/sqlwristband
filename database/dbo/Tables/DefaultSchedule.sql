CREATE TABLE [dbo].[DefaultSchedule]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1), 
    [MetricGroupId] INT NOT NULL CONSTRAINT FK_DefaultSchedule_MetricGroups FOREIGN KEY REFERENCES dbo.MetricGroups (Id), 
    [OffsetInSecondsFromMidnight] INT NOT NULL, 
    [IntervalInSeconds] INT NOT NULL, 
    [RetentionPeriodInHours] INT NOT NULL
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'DefaultSchedule',
	@name = N'Revision',
	@value = 1
GO