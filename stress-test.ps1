# Raft KV Store Stress Test Script
# Tests high concurrency and cache behavior

param(
    [string]$BaseUrl = "http://localhost:9081",
    [int]$ConcurrentOps = 50,
    [int]$Duration = 30  # seconds
)

$ErrorActionPreference = "Stop"
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$successCount = 0
$failCount = 0
$errors = @()

Write-Host "Starting stress test..." -ForegroundColor Cyan
Write-Host "Concurrent operations: $ConcurrentOps" -ForegroundColor Gray
Write-Host "Duration: $Duration seconds" -ForegroundColor Gray
Write-Host ""

# Run stress test
$jobs = @()
for ($i = 0; $i -lt $ConcurrentOps; $i++) {
    $jobs += Start-Job -ScriptBlock {
        param($url, $duration, $id)
        $success = 0
        $fail = 0
        $startTime = Get-Date
        
        while (((Get-Date) - $startTime).TotalSeconds -lt $duration) {
            try {
                $key = "stress-$id-$(Get-Random)"
                $opType = Get-Random -Minimum 0 -Maximum 4
                
                switch ($opType) {
                    0 {  # PUT
                        Invoke-RestMethod -Uri "$url/kv/$key" -Method PUT -Body '{"value":"test"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
                        $success++
                    }
                    1 {  # GET
                        Invoke-RestMethod -Uri "$url/kv/$key" -Method GET -TimeoutSec 5 | Out-Null
                        $success++
                    }
                    2 {  # CAS
                        $body = "{`"key`":`"$key`",`"expectedVersion`":0,`"newValue`":`"new`"}"
                        Invoke-RestMethod -Uri "$url/txn/cas" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
                        $success++
                    }
                    3 {  # Lock/Unlock
                        $lockKey = "lock-$id"
                        $owner = "client-$id"
                        $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"$owner`"}"
                        Invoke-RestMethod -Uri "$url/txn/lock" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
                        Invoke-RestMethod -Uri "$url/txn/unlock" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
                        $success++
                    }
                }
            } catch {
                $fail++
            }
        }
        
        return @{ Success = $success; Fail = $fail }
    } -ArgumentList $BaseUrl, $Duration, $i
}

Write-Host "Running stress test with $ConcurrentOps concurrent workers..." -ForegroundColor Yellow

# Wait for all jobs
$jobs | Wait-Job -Timeout ($Duration + 30) | Out-Null

# Collect results
foreach ($job in $jobs) {
    $result = Receive-Job -Job $job
    $successCount += $result.Success
    $failCount += $result.Fail
    Remove-Job -Job $job
}

$stopwatch.Stop()
$totalOps = $successCount + $failCount
$opsPerSec = [math]::Round($totalOps / $stopwatch.Elapsed.TotalSeconds, 2)
$successRate = [math]::Round(($successCount / $totalOps) * 100, 2)

Write-Host "`n========================================" -ForegroundColor Blue
Write-Host "  Stress Test Results" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
Write-Host "Total operations: $totalOps" -ForegroundColor White
Write-Host "Successful: $successCount" -ForegroundColor Green
Write-Host "Failed: $failCount" -ForegroundColor Red
Write-Host "Success rate: $successRate%" -ForegroundColor $(if($successRate -ge 95){"Green"}elseif($successRate -ge 80){"Yellow"}else{"Red"})
Write-Host "Throughput: $opsPerSec ops/sec" -ForegroundColor Cyan
Write-Host "Duration: $($stopwatch.Elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Gray

if ($successRate -ge 95) {
    Write-Host "`nStress test PASSED!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`nStress test FAILED!" -ForegroundColor Red
    exit 1
}
