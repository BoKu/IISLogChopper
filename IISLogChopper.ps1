<#
Author: BoKu
Date: 2023-05-08
Version: 1.0
Description: Imports W3C Files into MSSQL. Currently only supports W3C format!
#>

Clear-Host
Write-Host "IISLogChopper Starting`n" -ForegroundColor Blue

<# This is the parent folder to the main log files, this should contain the sub folders for each IIS server #>
$PathToLogFiles = "$env:SystemDrive\inetpub\logs\LogFiles"


<# Can be IIS, W3C or NCSA. W3C is the default. #>
$LogFormat = "W3C"



<# The MSSQL database location to import the log files in to #>
$sqlServer = "SQLSERVER"
$sqlUID = "sa"
$sqlPWD = "sa"
$sqlDB = "IISLogChopper"


<# Only W3C is supported #>
if($LogFormat -ne "W3C"){ return }


try{
    
    $sw = [Diagnostics.Stopwatch]::StartNew()

    <# Make sure there are log files to process #>
    Write-Host "Checking for $LogFormat files in $PathToLogFiles" -ForegroundColor Yellow -NoNewline
    $FileCount = 0
    Get-ChildItem -Path $PathToLogFiles -Filter *.log -Recurse -File -Name| ForEach-Object {
        #Write-Host [System.IO.Path]::GetFileNameWithoutExtension($_)
        $FileCount += 1
    }

    if( $FileCount -gt 0 ){
        Write-Host ": $FileCount files found`n" -ForegroundColor Green
    } else {
        Write-Host ": $FileCount files found`n" -ForegroundColor Red
        return
    }
    
    <# Database Connect and Table Check #>
    Write-Host "Attempting to connect to SQL Server $sqlServer" -ForegroundColor Yellow -NoNewline
    try{
        $sqlConn = New-Object System.Data.SqlClient.SqlConnection
        $sqlConn.ConnectionString = "Server=$sqlServer;UID=$sqlUID;PWD=$sqlPWD;App=IISLogChopper;Database=master;"
        $sqlConn.Open()
        Write-Host ": Connected`n" -ForegroundColor Green
    } catch {
        Write-Host ": Unable to Connect`n" -ForegroundColor Red
        return
    }



    Write-Host "Checking to see if both the database $sqlDB and table Logs_$LogFormat exist" -ForegroundColor Yellow -NoNewline
    $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $sqlCmd.Connection = $sqlConn
    $query = “SELECT ISNULL(CONVERT(BIGINT,DB_ID('$sqlDB')),0) * ISNULL(CONVERT(BIGINT,OBJECT_ID('$sqlDB..Logs_$LogFormat')),0) [DBandTBExists]”
    $sqlCmd.CommandText = $query

    $sqlAdp = New-Object System.Data.SqlClient.SqlDataAdapter $sqlCmd
    $sqlData = New-Object System.Data.DataSet
    $sqlAdp.Fill($sqlData) | Out-Null
    $sqlCmd.Dispose();

    if( $sqlData.Tables[0].DBandTBExists -eq "0" ){
        Write-Host ": One of not found!`n" -ForegroundColor Red
        Write-Host "Will now create the $sqlDB database and the table Logs_$LogFormat`n" -ForegroundColor Yellow
    
        Write-Host "Creating database $sqlDB (if missing)" -ForegroundColor Yellow -NoNewline
        try{
            $query = Get-Content "$PSScriptRoot\$sqlDB.sql"
            $query -split "GO;" | ForEach-Object{
                if($_ -ne "")
                {
                    $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
                    $sqlCmd.CommandText = $_ 
                    $sqlCmd.Connection = $sqlConn 
                    $sqlCmd.ExecuteNonQuery() | Out-Null
                }
            }
            Write-Host ": Complete`n" -ForegroundColor Green
        } catch {
            Write-Host ": Failed`n" -ForegroundColor Red
            return
        }
        $sqlCmd.Dispose();

        Write-Host "Creating table Logs_$LogFormat (if missing)" -ForegroundColor Yellow -NoNewline
        try{
            $query = Get-Content "$PSScriptRoot\$LogFormat.sql"
            $query -split "GO;" | ForEach-Object{
                if($_ -ne "")
                {
                    $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
                    $sqlCmd.CommandText = $_ 
                    $sqlCmd.Connection = $sqlConn 
                    $sqlCmd.ExecuteNonQuery() | Out-Null
                }
            }
            Write-Host ": Complete`n" -ForegroundColor Green
    
        } catch {
            Write-Host ": Failed`n" -ForegroundColor Red
            return
        }
        $sqlCmd.Dispose();


        Write-Host "Double-checking to see if the database $sqlDB and table Logs_$LogFormat now exist" -ForegroundColor Yellow -NoNewline
        $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
        $sqlCmd.Connection = $sqlConn
        $query = “SELECT ISNULL(CONVERT(BIGINT,DB_ID('$sqlDB')),0) * ISNULL(CONVERT(BIGINT,OBJECT_ID('$sqlDB..Logs_$LogFormat')),0) [DBandTBExists]”
        $sqlCmd.CommandText = $query

        $sqlAdp = New-Object System.Data.SqlClient.SqlDataAdapter $sqlCmd
        $sqlData = New-Object System.Data.DataSet
        $sqlAdp.Fill($sqlData) | Out-Null
        $sqlCmd.Dispose();

        if( $sqlData.Tables[0].DBandTBExists -eq "0" ){
            Write-Host ": Not Found`n" -ForegroundColor Red
            return
        } else {
            Write-Host ": Found`n" -ForegroundColor Green
        }

    } else {
        Write-Host ": Both Exists`n" -ForegroundColor Green
    }



    <# Import the Log Files #>
    $CurrentFile = 0
    if($LogFormat -eq "W3C"){

        $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
        $sqlCmd.CommandText = "USE [$sqlDB]"
        $sqlCmd.Connection = $sqlConn 
        $sqlCmd.ExecuteNonQuery() | Out-Null
        $query = ""
        $sqlCmd.Dispose();
        Write-Host "Importing $FileCount Log Files`n" -ForegroundColor Yellow -NoNewline
        try{
            Get-ChildItem -Path $PathToLogFiles -Filter *.log -Recurse | ForEach-Object {

                $CurrentFile += 1
                Write-Host "`n($CurrentFile/$FileCount)" -ForegroundColor Yellow -NoNewline

                $CurrentFileFullName = $_.FullName
                $CurrentTrimmedPath = $CurrentFileFullName.Replace($PathToLogFiles, "")
                Write-Host ": Reading File " -ForegroundColor Yellow -NoNewline
                Write-Host "$CurrentTrimmedPath" -ForegroundColor Green

                $LogHeaders = @((Get-Content -Path $CurrentFileFullName -ReadCount 4 -TotalCount 4)[3].split(' ') | Where-Object { $_ -ne '#Fields:' })-Join "],[";
                $sqlHeader = "INSERT INTO [Logs_$LogFormat] ([" + $LogHeaders + "],[sid])"
                # Write-Host $sqlHeader -ForegroundColor Cyan

                # $LogData = Import-Csv -Delimiter ' ' -Header $LogHeaders -Path $CurrentFileFullName | Where-Object { $_.date -notlike '#*' }
                # write-host $LogData

                $CurrentFileReader = [System.IO.File]::OpenText($CurrentFileFullName)
                While ($FileLine = $CurrentFileReader.ReadLine())
                {
                    if($FileLine -like '#*'){
                        continue
                    }
                    $sqlRow = "VALUES ('" + $FileLine.Replace("'","''").Replace(" ","','") + "', getdate())"
                    # Write-Host $sqlRow -ForegroundColor DarkCyan

                    $query = $sqlHeader + $sqlRow

                    $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
                    $sqlCmd.CommandText = $query
                    $sqlCmd.Connection = $sqlConn 
                    $sqlCmd.ExecuteNonQuery() | Out-Null

                    $query = ""
                    $sqlCmd.Dispose();
                }
                $CurrentFileReader.Close()
            
                Write-Host "Attempted to append file name with " -ForegroundColor DarkYellow -NoNewline
                Write-Host ".IMPORTED" -ForegroundColor Yellow -NoNewline
                try{
                    Rename-Item -Path $CurrentFileFullName -NewName "$CurrentFileFullName.IMPORTED"
                    Write-Host ": Success`n" -ForegroundColor Green
                } catch {
                    Write-Host ": Failed`n" -ForegroundColor Red
                }

            }
        } catch {
            Write-Host "Failed!`n" -ForegroundColor Red
            return
        }
    }

    $sqlConn.Close()
    $sqlConn.Dispose()
} catch {
    Write-Host "An unknown error has occured" -ForegroundColor Red
} finally {
    $sw.Stop()
    Write-Host "`nIISLogChopper Finished`n" -ForegroundColor Blue
    Write-host "Time Taken" -ForegroundColor DarkCyan -NoNewline
    $sw.Elapsed
}
