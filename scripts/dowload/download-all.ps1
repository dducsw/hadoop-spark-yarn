$ErrorActionPreference = "Continue"

$downloadsDir = "docker\base\downloads"
if (!(Test-Path $downloadsDir)) {
    New-Item -ItemType Directory -Path $downloadsDir -Force | Out-Null
}

$files = @(
    @{
        Name = "hadoop-3.4.3.tar.gz"
        Size = 514917037
        Url  = "https://mirrors.huaweicloud.com/apache/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz"
    },
    @{
        Name = "spark-3.5.4-bin-hadoop3.tgz"
        Size = 400879762
        Url  = "https://mirrors.huaweicloud.com/apache/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz"
    },
    @{
        Name = "apache-hive-3.1.3-bin.tar.gz"
        Size = 326940667
        Url  = "https://mirrors.huaweicloud.com/apache/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz"
    },
    @{
        Name = "postgresql-42.7.3.jar"
        Size = 1087405
        Url  = "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
    },
    @{
        Name = "clickhouse-jdbc-0.6.3-all.jar"
        Size = 13589410
        Url  = "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar"
    }
)

foreach ($f in $files) {
    $target = Join-Path $downloadsDir $f.Name
    $isComplete = $false

    while (-not $isComplete) {
        if (Test-Path $target) {
            $currSize = (Get-Item $target).Length
            if ($currSize -ge $f.Size) {
                Write-Host "[OK] $($f.Name) is fully downloaded ($([math]::Round($currSize/1MB, 2)) MB)." -ForegroundColor Green
                $isComplete = $true
                break
            } else {
                $pct = [math]::Round(($currSize / $f.Size) * 100, 1)
                Write-Host "[RESUME] $($f.Name): $pct% ($([math]::Round($currSize/1MB, 1)) / $([math]::Round($f.Size/1MB, 1)) MB). Resuming with curl..." -ForegroundColor Yellow
            }
        } else {
            Write-Host "[DOWNLOAD] Starting $($f.Name)..." -ForegroundColor Cyan
        }

        & curl.exe -fSL --retry 10 --retry-connrefused --retry-delay 2 -C - -o $target $f.Url

        if ((Test-Path $target) -and ((Get-Item $target).Length -ge $f.Size)) {
            $isComplete = $true
            Write-Host "[DONE] $($f.Name) completed successfully!" -ForegroundColor Green
        } else {
            Write-Host "Connection interrupted. Retrying in 2 seconds..." -ForegroundColor Yellow
            Start-Sleep -Seconds 2
        }
    }
}

Write-Host "`nAll packages are 100% downloaded and ready for Docker build!" -ForegroundColor Green
