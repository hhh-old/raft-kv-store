# Raft KV Store Simple Test
param([string]$BaseUrl = "http://localhost:9081")

$passed = 0
$failed = 0

function Test-Step($name, $scriptblock) {
    try {
        $result = & $scriptblock
        if ($result) {
            Write-Host "[PASS] $name" -ForegroundColor Green
            $script:passed++
        } else {
            Write-Host "[FAIL] $name" -ForegroundColor Red
            $script:failed++
        }
    } catch {
        Write-Host "[FAIL] $name : $_" -ForegroundColor Red
        $script:failed++
    }
}

Write-Host "`n=== Raft KV Store Test ===" -ForegroundColor Cyan
Write-Host "URL: $BaseUrl`n"

# Wait for cluster
Write-Host "Waiting for cluster..." -NoNewline
for ($i = 0; $i -lt 30; $i++) {
    try {
        $r = Invoke-RestMethod -Uri "$BaseUrl/kv/stats" -Method GET -TimeoutSec 2
        if ($r.role) { Write-Host " OK ($($r.role))" -ForegroundColor Green; break }
    } catch {}
    Start-Sleep -Seconds 1
}

# Generate unique test keys
$ts = Get-Date -Format "HHmmss"
$testKey = "test$ts"
$casKey = "cas$ts"
$lockKey = "lock$ts"
Write-Host "Test keys: $testKey, $casKey, $lockKey" -ForegroundColor Gray

# Test 1: Basic PUT/GET
Test-Step "PUT operation" {
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"v1"}' -ContentType "application/json"
    $r.success
}

Start-Sleep -Milliseconds 500

Test-Step "GET operation" {
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET
    $r.value -eq "v1"
}

# Test 2: CAS
Test-Step "CAS prepare" {
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$casKey" -Method PUT -Body '{"value":"old"}' -ContentType "application/json"
    $r.success
}

Start-Sleep -Milliseconds 500

Test-Step "CAS success" {
    $body = "{`"key`":`"$casKey`",`"expectedValue`":`"old`",`"newValue`":`"new`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body $body -ContentType "application/json"
    $r.succeeded
}

Start-Sleep -Milliseconds 500

Test-Step "CAS verify" {
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv/$casKey" -Method GET
    $r.value -eq "new"
}

Test-Step "CAS fail (wrong expect)" {
    $body = "{`"key`":`"$casKey`",`"expectedValue`":`"wrong`",`"newValue`":`"x`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST -Body $body -ContentType "application/json"
    -not $r.succeeded
}

# Test 3: Lock
Test-Step "Lock acquire" {
    $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"c1`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST -Body $body -ContentType "application/json"
    $r.succeeded
}

Test-Step "Lock fail (already held)" {
    $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"c2`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST -Body $body -ContentType "application/json"
    -not $r.succeeded
}

Test-Step "Lock release wrong owner" {
    $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"c2`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/unlock" -Method POST -Body $body -ContentType "application/json"
    -not $r.succeeded
}

Test-Step "Lock release correct owner" {
    $body = "{`"lockKey`":`"$lockKey`",`"owner`":`"c1`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/txn/unlock" -Method POST -Body $body -ContentType "application/json"
    $r.succeeded
}

# Summary
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "Passed: $passed, Failed: $failed" -ForegroundColor $(if($failed -eq 0){"Green"}else{"Red"})
exit $failed
