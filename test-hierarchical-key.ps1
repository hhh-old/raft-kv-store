# 测试分层 key（带斜杠的key）
param([string]$BaseUrl = "http://localhost:9081")

$ErrorActionPreference = "Stop"
$passed = 0
$failed = 0

function Write-TestResult($name, $success, $details = "") {
    if ($success) {
        Write-Host "  [PASS] $name" -ForegroundColor Green
        $script:passed++
    } else {
        Write-Host "  [FAIL] $name" -ForegroundColor Red
        if ($details) { Write-Host "         $details" -ForegroundColor Yellow }
        $script:failed++
    }
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  分层 Key 测试（带斜杠的key）" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 测试1: PUT/GET 带斜杠的key
Write-Host "`n测试1: PUT/GET 带斜杠的key" -ForegroundColor Yellow
$key = "config/app/version"
$value = "v1.0.0"
try {
    # PUT
    $body = "{`"key`":`"$key`",`"value`":`"$value`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "PUT 分层key" ($r.success -eq $true) "key=$key"

    Start-Sleep -Milliseconds 200

    # GET
    $encodedKey = [System.Web.HttpUtility]::UrlEncode($key)
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv?keyParam=$encodedKey" -Method GET -TimeoutSec 5
    Write-TestResult "GET 分层key" ($r2.value -eq $value) "expected=$value, got=$($r2.value)"
} catch {
    Write-TestResult "分层key测试" $false $_.Exception.Message
}

# 测试2: DELETE 带斜杠的key
Write-Host "`n测试2: DELETE 带斜杠的key" -ForegroundColor Yellow
$key = "config/app/database"
try {
    # PUT
    $body = "{`"key`":`"$key`",`"value`":`"localhost:3306`"}"
    Invoke-RestMethod -Uri "$BaseUrl/kv" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
    Start-Sleep -Milliseconds 200

    # DELETE
    $encodedKey = [System.Web.HttpUtility]::UrlEncode($key)
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv?keyParam=$encodedKey&requestId=req-del-001" -Method DELETE -TimeoutSec 5
    Write-TestResult "DELETE 分层key" ($r.success -eq $true)

    Start-Sleep -Milliseconds 200

    # 验证已删除
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv?keyParam=$encodedKey" -Method GET -TimeoutSec 5
    Write-TestResult "验证删除" ($r2.success -eq $false -or $r2.value -eq $null)
} catch {
    Write-TestResult "DELETE 分层key" $false $_.Exception.Message
}

# 测试3: 多层嵌套key
Write-Host "`n测试3: 多层嵌套key" -ForegroundColor Yellow
$key = "a/b/c/d/e"
$value = "nested-value"
try {
    $body = "{`"key`":`"$key`",`"value`":`"$value`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "PUT 多层嵌套key" ($r.success -eq $true)

    Start-Sleep -Milliseconds 200

    $encodedKey = [System.Web.HttpUtility]::UrlEncode($key)
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv?keyParam=$encodedKey" -Method GET -TimeoutSec 5
    Write-TestResult "GET 多层嵌套key" ($r2.value -eq $value)
} catch {
    Write-TestResult "多层嵌套key" $false $_.Exception.Message
}

# 测试4: 简单key（向后兼容）
Write-Host "`n测试4: 简单key（向后兼容）" -ForegroundColor Yellow
$key = "simplekey"
$value = "simple-value"
try {
    $body = "{`"key`":`"$key`",`"value`":`"$value`"}"
    $r = Invoke-RestMethod -Uri "$BaseUrl/kv" -Method PUT -Body $body -ContentType "application/json" -TimeoutSec 5
    Write-TestResult "PUT 简单key" ($r.success -eq $true)

    Start-Sleep -Milliseconds 200

    $encodedKey = [System.Web.HttpUtility]::UrlEncode($key)
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv?keyParam=$encodedKey" -Method GET -TimeoutSec 5
    Write-TestResult "GET 简单key" ($r2.value -eq $value)
} catch {
    Write-TestResult "简单key" $false $_.Exception.Message
}

# 总结
Write-Host "`n========================================" -ForegroundColor Blue
Write-Host "  测试结果汇总" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
$total = $passed + $failed
Write-Host "总测试数: $total" -ForegroundColor White
Write-Host "通过: $passed" -ForegroundColor Green
Write-Host "失败: $failed" -ForegroundColor Red

if ($failed -eq 0) {
    Write-Host "`n所有测试通过！分层key支持正常。" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`n部分测试失败。" -ForegroundColor Red
    exit 1
}
