CREATE FUNCTION [dbo].[GetTargetSchemaName]
(
	@TargetId int
)
RETURNS VARCHAR(12)
AS
BEGIN
	RETURN N'tgt' + SUBSTRING(N'000000000', 1, 9 - LEN(CAST(@TargetId as VARCHAR(9)))) + CAST(@TargetId as NVARCHAR(9))
END
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Function',
	@level1name = N'GetTargetSchemaName',
	@name = N'Revision',
	@value = 1
GO