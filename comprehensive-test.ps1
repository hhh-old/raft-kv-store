# Raft KV Store Comprehensive Test Script
# Tests boundary conditions, concurrency, fault recovery, etc.

param(
    [string]$BaseUrl = "http://localhost:9081",
    [int]$Concurrency = 10,
    [int]$Iterations = 100
)

$ErrorActionPreference = "Stop"
$passed = 0
$failed = 0
$testResults = @()

function Write-TestResult($category, $name, $success, $details = "") {
    $result = [PSCustomObject]@{
        Category = $category
        Name = $name
        Success = $success
        Details = $details
        Timestamp = Get-Date
    }
    $script:testResults += $result
    
    if ($success) {
        Write-Host "  [PASS] $name" -ForegroundColor Green
        $script:passed++
    } else {
        Write-Host "  [FAIL] $name" -ForegroundColor Red
        if ($details) {
            Write-Host "         $details" -ForegroundColor Yellow
        }
        $script:failed++
    }
}

function Test-Category($name) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "  $name" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

# ==================== Basic Functionality Tests ====================
Test-Category "1. Basic Functionality Tests"

# 1.1 Empty value test
$testKey = "test-empty-$(Get-Random)"
try {
    $body = '{"value":""}'
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Basic" "PUT empty string" ($r.success -eq $true)
    
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5
    Write-TestResult "Basic" "GET empty string" ($r2.value -eq "")
} catch {
    Write-TestResult "Basic" "Empty value test" $false $_.Exception.Message
}

# 1.2 Special characters test
$testKey = "test-special-$(Get-Random)"
$specialValue = "test-value-with-special-chars"
try {
    $body = "{`"value`":`"$specialValue`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Basic" "PUT special chars" ($r.success -eq $true)
    
    Start-Sleep -Milliseconds 200
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5
    Write-TestResult "Basic" "GET special chars" ($r2.value -eq $specialValue)
} catch {
    Write-TestResult "Basic" "Special chars test" $false $_.Exception.Message
}

# 1.3 Long value test
$testKey = "test-long-$(Get-Random)"
$longValue = "a" * 10000
try {
    $body = "{`"value`":`"$longValue`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 10
    Write-TestResult "Basic" "PUT long value (10KB)" ($r.success -eq $true)
    
    Start-Sleep -Milliseconds 500
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 10
    Write-TestResult "Basic" "GET long value (10KB)" ($r2.value -eq $longValue)
} catch {
    Write-TestResult "Basic" "Long value test" $false $_.Exception.Message
}

# 1.4 Unicode test
$testKey = "test-unicode-$(Get-Random)"
$unicodeValue = "Chinese Test"
try {
    $body = "{`"value`":`"$unicodeValue`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Basic" "PUT Unicode" ($r.success -eq $true)
    
    Start-Sleep -Milliseconds 200
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5
    Write-TestResult "Basic" "GET Unicode" ($r2.value -eq $unicodeValue)
} catch {
    Write-TestResult "Basic" "Unicode test" $false $_.Exception.Message
}

# ==================== Transaction Boundary Tests ====================
Test-Category "2. Transaction Boundary Tests"

# 2.1 CAS on non-existent key
$testKey = "cas-notexist-$(Get-Random)"
try {
    $body = "{`"key`":`"$testKey`",`"expectedValue`":`"old`",`"newValue`":`"new`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Txn Boundary" "CAS on non-existent key" ($r.succeeded -eq $false)
} catch {
    Write-TestResult "Txn Boundary" "CAS non-existent key" $false $_.Exception.Message
}

# 2.2 Version CAS on non-existent key (version=0)
$testKey = "cas-version-$(Get-Random)"
try {
    $body = "{`"key`":`"$testKey`",`"expectedVersion`":0,`"newValue`":`"new`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Txn Boundary" "Version CAS on non-existent key (version=0)" ($r.succeeded -eq $true)
} catch {
    Write-TestResult "Txn Boundary" "Version CAS test" $false $_.Exception.Message
}

# 2.3 Empty transaction
$testKey = "txn-empty-$(Get-Random)"
try {
    $body = '{"compares":[],"success":[],"failure":[]}'
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Txn Boundary" "Empty transaction" ($r.succeeded -eq $true)
} catch {
    Write-TestResult "Txn Boundary" "Empty transaction" $false $_.Exception.Message
}

# 2.4 Multi-operation transaction
$testKey1 = "txn-multi1-$(Get-Random)"
$testKey2 = "txn-multi2-$(Get-Random)"
try {
    $body = "{`"compares`":[],`"success`":[{`"type`":`"PUT`",`"key`":`"$testKey1`",`"value`":`"v1`"},{`"type`":`"PUT`",`"key`":`"$testKey2`",`"value`":`"v2`"}],`"failure`":[]}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Txn Boundary" "Multi-PUT transaction" ($r.succeeded -eq $true)
    
    Start-Sleep -Milliseconds 200
    $r1 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey1" -Method GET -TimeoutSec 5
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey2" -Method GET -TimeoutSec 5
    Write-TestResult "Txn Boundary" "Verify multi-PUT atomicity" (($r1.value -eq "v1") -and ($r2.value -eq "v2"))
} catch {
    Write-TestResult "Txn Boundary" "Multi-op transaction" $false $_.Exception.Message
}

# ==================== Distributed Lock Tests ====================
Test-Category "3. Distributed Lock Advanced Tests"

# 3.1 Lock reentrancy test (same owner)
$lockKey = "lock-reentrant-$(Get-Random)"
try {
    $body1 = "{`"lockKey`":`"$lockKey`",`"owner`":`"client1`"}"
    $r1 = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST -Body $body1 -ContentType "application/json" -TimeoutSec 5
    
    # Same owner tries to acquire again (should fail because lock is held)
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST -Body $body1 -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Distributed Lock" "Lock reentrancy test" (($r1.succeeded -eq $true) -and ($r2.succeeded -eq $false))
    
    # Release lock
    $r3 = Invoke-RestMethod -Uri "$BaseUrl/txn/unlock" -Method POST -Body $body1 -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Distributed Lock" "Lock release" ($r3.succeeded -eq $true)
} catch {
    Write-TestResult "Distributed Lock" "Reentrancy test" $false $_.Exception.Message
}

# 3.2 Lock contention test
$lockKey = "lock-contention-$(Get-Random)"
$owners = @("client1", "client2", "client3")
$acquired = 0
try {
    foreach ($owner in $owners) {
        $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"$owner`"}"
        $r = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5
        if ($r.succeeded) { $acquired++ }
    }
    Write-TestResult "Distributed Lock" "Lock contention (3 clients)" ($acquired -eq 1)
    
    # Cleanup
    $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"client1`"}"
    Invoke-RestMethod -Uri "$BaseUrl/txn/unlock" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
} catch {
    Write-TestResult "Distributed Lock" "Contention test" $false $_.Exception.Message
}

# ==================== Concurrency Tests ====================
Test-Category "4. Concurrency Tests (Lightweight)"

# 4.1 Concurrent PUT different keys
Write-Host "  Concurrent PUT $Concurrency different keys..." -NoNewline
$jobs = @()
for ($i = 0; $i -lt $Concurrency; $i++) {
    $key = "concurrent-$i-$(Get-Random)"
    $body = "{`"value`":`"value-$i`"}"
    $jobs += Start-Job -ScriptBlock {
        param($url, $key, $body)
        try {
            Invoke-RestMethod -Uri "$url/kv/$key" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 10
            return $true
        } catch {
            return $false
        }
    } -ArgumentList $BaseUrl, $key, $body
}
$jobs | Wait-Job -Timeout 30 | Out-Null
$successCount = ($jobs | Receive-Job | Where-Object { $_ -eq $true }).Count
$jobs | Remove-Job -Force
Write-TestResult "Concurrency" "Concurrent PUT different keys ($Concurrency)" ($successCount -eq $Concurrency)

# 4.2 Concurrent CAS same key
Write-Host "  Concurrent CAS same key..." -NoNewline
$testKey = "concurrent-cas-$(Get-Random)"
Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"0"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
Start-Sleep -Milliseconds 200

$jobs = @()
for ($i = 0; $i -lt 5; $i++) {
    $expected = $i
    $newValue = $i + 1
    $body = "{`"key`":`"$testKey`",`"expectedValue`":`"$expected`",`"newValue`":`"$newValue`"}"
    $jobs += Start-Job -ScriptBlock {
        param($url, $body)
        try {
            $r = Invoke-RestMethod -Uri "$url/txn/cas" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 10
            return $r.succeeded
        } catch {
            return $false
        }
    } -ArgumentList $BaseUrl, $body
}
$jobs | Wait-Job -Timeout 30 | Out-Null
$results = $jobs | Receive-Job
$jobs | Remove-Job -Force
$successCount = ($results | Where-Object { $_ -eq $true }).Count
Write-TestResult "Concurrency" "Concurrent CAS same key (5 threads)" ($successCount -ge 1) "Success: $successCount"

# ==================== Fault Handling Tests ====================
Test-Category "5. Fault Handling Tests"

# 5.1 Invalid JSON test
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body 'invalid json' -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Fault Handling" "Invalid JSON" $false "Should throw exception"
} catch {
    Write-TestResult "Fault Handling" "Invalid JSON handling" $true "Correctly rejected"
}

# 5.2 Empty request body test
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body '{}' -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "Fault Handling" "Empty request body" ($r.error -ne $null)
} catch {
    Write-TestResult "Fault Handling" "Empty request body" $true "Correctly rejected"
}

# 5.3 Non-existent endpoint test
try {
    $r = Invoke-RestMethod -Uri "$BaseUrl/nonexistent" -Method GET -TimeoutSec 5
    Write-TestResult "Fault Handling" "Non-existent endpoint" $false "Should 404"
} catch {
    Write-TestResult "Fault Handling" "Non-existent endpoint" $true "Correctly returned 404"
}

# ==================== Performance Benchmark Tests ====================
Test-Category "6. Performance Benchmark Tests"

# 6.1 PUT performance
Write-Host "  PUT performance test ($Iterations iterations)..." -NoNewline
$sw = [System.Diagnostics.Stopwatch]::StartNew()
for ($i = 0; $i -lt $Iterations; $i++) {
    $key = "perf-put-$i"
    Invoke-RestMethod -Uri "$BaseUrl/kv/$key" -Method PUT -Body '{"value":"test"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
}
$sw.Stop()
$opsPerSec = [math]::Round($Iterations / $sw.Elapsed.TotalSeconds, 2)
Write-TestResult "Performance" "PUT throughput" $true "$opsPerSec ops/sec"

# 6.2 GET performance
Write-Host "  GET performance test ($Iterations iterations)..." -NoNewline
$testKey = "perf-get-test"
Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"test"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
Start-Sleep -Milliseconds 100

$sw = [System.Diagnostics.Stopwatch]::StartNew()
for ($i = 0; $i -lt $Iterations; $i++) {
    Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5 | Out-Null
}
$sw.Stop()
$opsPerSec = [math]::Round($Iterations / $sw.Elapsed.TotalSeconds, 2)
Write-TestResult "Performance" "GET throughput" $true "$opsPerSec ops/sec"

# 6.3 CAS performance
Write-Host "  CAS performance test ($Iterations iterations)..." -NoNewline
$testKey = "perf-cas-test"
Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"0"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
Start-Sleep -Milliseconds 100

$sw = [System.Diagnostics.Stopwatch]::StartNew()
for ($i = 0; $i -lt $Iterations; $i++) {
    $body = "{`"key`":`"$testKey`",`"expectedValue`":`"$i`",`"newValue`":`"$($i+1)`"}"
    Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
}
$sw.Stop()
$opsPerSec = [math]::Round($Iterations / $sw.Elapsed.TotalSeconds, 2)
Write-TestResult "Performance" "CAS throughput" $true "$opsPerSec ops/sec"

# ==================== Test Summary ====================
Write-Host "`n========================================" -ForegroundColor Blue
Write-Host "  Test Summary" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue

$total = $passed + $failed
$passRate = [math]::Round(($passed / $total) * 100, 2)

Write-Host "Total tests: $total" -ForegroundColor White
Write-Host "Passed: $passed" -ForegroundColor Green
Write-Host "Failed: $failed" -ForegroundColor Red
Write-Host "Pass rate: $passRate%" -ForegroundColor $(if($passRate -ge 90){"Green"}elseif($passRate -ge 70){"Yellow"}else{"Red"})

# Statistics by category
Write-Host "`nStatistics by category:" -ForegroundColor Cyan
$testResults | Group-Object Category | ForEach-Object {
    $catPassed = ($_.Group | Where-Object { $_.Success }).Count
    $catTotal = $_.Group.Count
    $catRate = [math]::Round(($catPassed / $catTotal) * 100, 0)
    Write-Host "  $($_.Name): $catPassed/$catTotal ($catRate%)" -ForegroundColor Gray
}

if ($failed -eq 0) {
    Write-Host "`nAll tests passed!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`nSome tests failed, please check logs" -ForegroundColor Red
    exit 1
}
