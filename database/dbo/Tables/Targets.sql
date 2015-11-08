CREATE TABLE [dbo].[Targets]
(
	[Id] INT NOT NULL PRIMARY KEY IDENTITY(0, 1),
    [ServerName] NVARCHAR(512) NOT NULL,
	[IsSqlAuthentication] BIT NOT NULL DEFAULT 0,
    [Username] NVARCHAR(64) NULL,
    [Password] VARCHAR(64) NULL,
    [DateAdded] DATETIME2(0) NOT NULL DEFAULT GETDATE()
)
GO
EXEC sp_addextendedproperty
	@level0type = 'Schema',
	@level0name = N'dbo',
	@level1type = 'Table',
	@level1name = N'Targets',
	@name = N'Revision',
	@value = 2
GO