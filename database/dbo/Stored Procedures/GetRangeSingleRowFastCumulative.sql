CREATE procedure [dbo].[GetRangeSingleRowFastCumulative]
	@tablename sysname,
	@columnname sysname,
	@start_dt datetime2(0),
	@end_dt datetime2(0),
	@interval smallint
AS
BEGIN

DECLARE @sqlStmt nvarchar(max)

SET @sqlStmt = N'WITH cteNumbered AS (
	select ROW_NUMBER() OVER(order by dt) rownum, dt, [' + @columnname + N'] _value
	from ' + @tablename + N'
	where dt between @startDate and @endDate
)

select STUFF(CAST((select REPLACE(REPLACE('',["'' + CONVERT(CHAR(16), DATEADD(minute, -1*(DATEPART(minute, lft.dt) % @interval), lft.dt), 120) + ''",'' + CAST(CAST(AVG((rgh._value - lft._value)/DATEDIFF(second, lft.dt, rgh.dt)) as NUMERIC(19, 2)) as VARCHAR(64)) + '']'', ''0]'', '']''), ''.0]'', '']'')
from cteNumbered lft
	inner join cteNumbered rgh
		on lft.rownum + 1 = rgh.rownum and lft.dt < rgh.dt
group by CONVERT(CHAR(16), DATEADD(minute, -1*(DATEPART(minute, lft.dt) % @interval), lft.dt), 120)
order by CONVERT(CHAR(16), DATEADD(minute, -1*(DATEPART(minute, lft.dt) % @interval), lft.dt), 120)
for xml path('''')) as varchar(max)), 1, 1, '''')';

EXEC sp_executesql
	@sqlStmt,
	N'@interval as smallint, @startDate as datetime2(0), @endDate as datetime2(0)',
	@interval = @interval,
	@startDate = @start_dt,
	@endDate = @end_dt
END
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Procedure',
	@level1name = N'GetRangeSingleRowFastCumulative',
	@name = N'Revision',
	@value = 1
GO