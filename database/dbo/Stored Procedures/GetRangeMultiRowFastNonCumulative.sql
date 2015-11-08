CREATE PROCEDURE [dbo].[GetRangeMultiRowFastNonCumulative]
 @dataTable sysname,
 @dictionary sysname,
 @start_dt datetime2(0),
 @end_dt datetime2(0),
 @interval smallint,
 @metricColumn varchar(255),
 @numOfRowsToReturn tinyint,
 @exclusionColumn varchar(255),
 @excludedValues varchar(2048) -- Optional. ~ separated list of values to exclude from the output (ex:'HADR_FILESTREAM_IOMGR_IOCOMPLETION~REQUEST_FOR_DEADLOCK_SEARCH')
AS
BEGIN

-- Here the procedure starts
DECLARE
 @notInClause nvarchar(max),
 @sqlStmt nvarchar(max)

IF LEN(@exclusionColumn) > 0 AND LEN(@excludedValues) > 0
 SET @notInClause = ' diff inner join ' + @dictionary + ' dict on diff.dictId = dict.id where dict.[' + @exclusionColumn + '] not in (''' + REPLACE(@excludedValues, '~', ''',''') + ''')'
ELSE
 SET @notInClause = ''

SET @sqlStmt = 
'WITH cteNumbered AS (
 select ROW_NUMBER() OVER(PARTITION BY dictId ORDER BY dt) rownum, dictId, dt,
  [' + @metricColumn + '] as _value
 from ' + @dataTable + '
 where dt between @startDate and @endDate
),
cteTopIds AS (
 select TOP ' + CAST(@numOfRowsToReturn as NVARCHAR(5)) + ' dictId
 from cteNumbered ' + @notInClause + '
 group by dictId
 order by SUM(_value) desc
)

select ''{"series":"'' + REPLACE(dict.' + @exclusionColumn + ', ''\'',''\\'') + ''","data":['' + 
 STUFF(CAST((select REPLACE(REPLACE('',["'' + CONVERT(CHAR(16), DATEADD(minute, -1*(DATEPART(minute, data.dt) % @interval), data.dt), 120) + ''",'' + CAST(CAST(AVG(data._value) as NUMERIC(19, 2)) as VARCHAR(64)) + '']'', ''0]'', '']''), ''.0]'', '']'')
from cteNumbered data
where data.dictId = topIds.dictId and data.dt between @startDate and @endDate
group by CONVERT(CHAR(16), DATEADD(minute, -1*(DATEPART(minute, data.dt) % @interval), data.dt), 120)
order by CONVERT(CHAR(16), DATEADD(minute, -1*(DATEPART(minute, data.dt) % @interval), data.dt), 120)
  for xml path('''')) as varchar(max)), 1, 1, ''''
 ) + '']}'' [jsonstr]
from cteTopIds topIds
 inner join ' + @dictionary + ' dict
  on topIds.dictId = dict.id'

IF CHARINDEX('slow', @dictionary) > 0
 SET @sqlStmt = @sqlStmt + ' and dict.endDate is null'

EXEC sp_executesql
 @sqlStmt,
 N'@interval as smallint, @startDate as datetime2(0), @endDate as datetime2(0)',
 @interval = @interval,
 @startDate = @start_dt,
 @endDate = @end_dt
END
GO

EXEC sys.sp_addextendedproperty @name=N'Revision', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'PROCEDURE',@level1name=N'GetRangeMultiRowFastNonCumulative'
GO