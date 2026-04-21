# Raft KV Store 完整功能测试脚本
# 包含：基础KV操作、CAS、分布式锁、事务、Watch功能测试

param(
    [string]$BaseUrl = "http://localhost:9081",
    [int]$WaitSeconds = 3
)

$ErrorActionPreference = "Stop"
$passed = 0
$failed = 0

function Write-TestHeader($title) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Write-TestResult($name, $success, $details = "") {
    if ($success) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $script:passed++
    } else {
        Write-Host "[FAIL] $name" -ForegroundColor Red
        if ($details) {
            Write-Host "       Details: $details" -ForegroundColor Yellow
        }
        $script:failed++
    }
}

function Wait-ForCluster($url, $maxAttempts = 30) {
    Write-Host "Waiting for cluster to be ready..." -NoNewline
    for ($i = 0; $i -lt $maxAttempts; $i++) {
        try {
            $response = Invoke-RestMethod -Uri "$url/kv/stats" -Method GET -TimeoutSec 2
            if ($response.role -eq "LEADER" -or $response.role -eq "FOLLOWER") {
                Write-Host " OK (Role: $($response.role))" -ForegroundColor Green
                return $true
            }
        } catch {
            Write-Host "." -NoNewline
        }
        Start-Sleep -Seconds 1
    }
    Write-Host " TIMEOUT" -ForegroundColor Red
    return $false
}

# ==================== 测试开始 ====================
Write-Host "`nRaft KV Store 功能测试" -ForegroundColor Blue
Write-Host "Base URL: $BaseUrl" -ForegroundColor Gray
Write-Host "Start time: $(Get-Date)" -ForegroundColor Gray

# 等待集群就绪
if (-not (Wait-ForCluster $BaseUrl)) {
    Write-Host "Cluster not ready, exiting..." -ForegroundColor Red
    exit 1
}

# 清理测试数据
Write-Host "`nCleaning test data..." -NoNewline
try {
    $keys = @("test-key", "cas-key", "lock-key", "txn-key1", "txn-key2", "watch-key")
    foreach ($key in $keys) {
        try { Invoke-RestMethod -Uri "$BaseUrl/kv/$key" -Method DELETE -TimeoutSec 2 | Out-Null } catch {}
    }
    Write-Host " OK" -ForegroundColor Green
} catch {
    Write-Host " (some keys may not exist)" -ForegroundColor Yellow
}

Start-Sleep -Seconds 1

# ==================== 基础KV测试 ====================
Write-TestHeader "1. 基础KV操作测试"

# Test 1.1: PUT操作
$testName = "PUT操作"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/test-key" -Method PUT `
        -Body '{"value":"test-value","requestId":"test-001"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.success -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 1.2: GET操作
$testName = "GET操作"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/test-key" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.success -eq $true -and $response.value -eq "test-value")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 1.3: GET ALL操作
$testName = "GET ALL操作"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response -ne $null -and $response.Count -ge 0)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 1.4: DELETE操作
$testName = "DELETE操作"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/test-key" -Method DELETE -TimeoutSec 5
    Write-TestResult $testName ($response.success -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 1.5: 验证删除
$testName = "验证删除"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/test-key" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.success -eq $true -and $response.value -eq $null)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# ==================== CAS测试 ====================
Write-TestHeader "2. CAS (Compare-And-Swap) 测试"

# Test 2.1: 设置初始值
$testName = "CAS准备：设置初始值"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/cas-key" -Method PUT `
        -Body '{"value":"initial"}' -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.success -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 2.2: CAS成功 - 期望值正确
$testName = "CAS成功（期望值正确）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST `
        -Body '{"key":"cas-key","expectedValue":"initial","newValue":"updated"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 2.3: 验证CAS更新成功
$testName = "验证CAS更新成功"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/cas-key" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.value -eq "updated")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 2.4: CAS失败 - 期望值错误
$testName = "CAS失败（期望值错误）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/cas" -Method POST `
        -Body '{"key":"cas-key","expectedValue":"wrong-value","newValue":"should-fail"}' `
        -ContentType "application/json" -TimeoutSec 5
    # CAS应该失败，但应该返回当前值
    Write-TestResult $testName ($response.succeeded -eq $false -and $response.results."0".value -eq "updated")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 2.5: 验证值未改变
$testName = "验证CAS失败后值未改变"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/cas-key" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.value -eq "updated")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# ==================== 分布式锁测试 ====================
Write-TestHeader "3. 分布式锁测试"

# Test 3.1: 获取锁
$testName = "获取锁（应该成功）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST `
        -Body '{"lockKey":"resource-1","owner":"client-1"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 3.2: 重复获取锁（应该失败）
$testName = "重复获取锁（应该失败）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST `
        -Body '{"lockKey":"resource-1","owner":"client-2"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $false -and $response.results."0".value -eq "client-1")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 3.3: 错误owner释放锁（应该失败）
$testName = "错误owner释放锁（应该失败）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/unlock" -Method POST `
        -Body '{"lockKey":"resource-1","owner":"client-2"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $false)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 3.4: 正确owner释放锁（应该成功）
$testName = "正确owner释放锁（应该成功）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/unlock" -Method POST `
        -Body '{"lockKey":"resource-1","owner":"client-1"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 3.5: 释放后重新获取锁（应该成功）
$testName = "释放后重新获取锁（应该成功）"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn/lock" -Method POST `
        -Body '{"lockKey":"resource-1","owner":"client-2"}' `
        -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# ==================== 事务测试 ====================
Write-TestHeader "4. 多键事务测试"

# Test 4.1: 原子性多键更新
$testName = "原子性多键更新"
try {
    $txnBody = @{
        compares = @()
        success = @(
            @{ type = "PUT"; key = "txn-key1"; value = "value1" }
            @{ type = "PUT"; key = "txn-key2"; value = "value2" }
        )
        failure = @()
        requestId = "txn-test-001"
    } | ConvertTo-Json -Depth 3
    
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST `
        -Body $txnBody -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 4.2: 验证事务更新
$testName = "验证事务更新 key1"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/txn-key1" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.value -eq "value1")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

$testName = "验证事务更新 key2"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/txn-key2" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.value -eq "value2")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 4.3: 带条件的事务（条件满足）
$testName = "带条件的事务（条件满足）"
try {
    $txnBody = @{
        compares = @(
            @{ target = "VALUE"; key = "txn-key1"; op = "EQUAL"; value = "value1" }
        )
        success = @(
            @{ type = "PUT"; key = "txn-key1"; value = "updated-value1" }
        )
        failure = @()
        requestId = "txn-test-002"
    } | ConvertTo-Json -Depth 3
    
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST `
        -Body $txnBody -ContentType "application/json" -TimeoutSec 5
    Write-TestResult $testName ($response.succeeded -eq $true)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

Start-Sleep -Milliseconds 500

# Test 4.4: 验证条件事务更新
$testName = "验证条件事务更新"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/txn-key1" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response.value -eq "updated-value1")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# Test 4.5: 带条件的事务（条件不满足）
$testName = "带条件的事务（条件不满足）"
try {
    $txnBody = @{
        compares = @(
            @{ target = "VALUE"; key = "txn-key1"; op = "EQUAL"; value = "wrong-value" }
        )
        success = @(
            @{ type = "PUT"; key = "txn-key1"; value = "should-not-happen" }
        )
        failure = @(
            @{ type = "GET"; key = "txn-key1" }
        )
        requestId = "txn-test-003"
    } | ConvertTo-Json -Depth 3
    
    $response = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST `
        -Body $txnBody -ContentType "application/json" -TimeoutSec 5
    # 应该失败，但执行了failure操作
    Write-TestResult $testName ($response.succeeded -eq $false -and $response.results."0".value -eq "updated-value1")
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# ==================== 集群状态测试 ====================
Write-TestHeader "5. 集群状态测试"

# Test 5.1: 获取集群状态
$testName = "获取集群状态"
try {
    $response = Invoke-RestMethod -Uri "$BaseUrl/kv/stats" -Method GET -TimeoutSec 5
    Write-TestResult $testName ($response -ne $null -and $response.role -ne $null)
} catch {
    Write-TestResult $testName $false $_.Exception.Message
}

# ==================== 测试总结 ====================
Write-TestHeader "测试总结"

$total = $passed + $failed
Write-Host "Total Tests: $total" -ForegroundColor Blue
Write-Host "Passed: $passed" -ForegroundColor Green
Write-Host "Failed: $failed" -ForegroundColor Red

if ($failed -eq 0) {
    Write-Host "`nAll tests passed!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`nSome tests failed!" -ForegroundColor Red
    exit 1
}
