Param (
	[parameter(Mandatory=$true)] [String] $paramInputScript,
	[parameter(Mandatory=$true)] [String] $paramOutputScript
)

Write-Host "Reading [$paramInputScript] file";
$inputScript = Get-Content $paramInputScript -Encoding UTF8;

$include = $false;
$outputScript = @();

ForEach ($line in $inputScript)
{
    if (!$include)
    {
        if ($line -ne $null -and $line.Contains('USE [$(DatabaseName)];'))
        {
            $include = $true;
        }
    }
    else
    {
	if ($line -ne $null -and $line.Contains('VarDecimalSupported'))
	{
		break;
	}
        $outputScript += $line;
    }
}

Write-Host "Saving results to [$paramOutputScript] file";
$outputScript | Set-Content -Path $paramOutputScript -Encoding Unicode -Force;