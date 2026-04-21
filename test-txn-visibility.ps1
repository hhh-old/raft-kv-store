# 测试事务内可见性 - 对齐 etcd 语义
# 验证事务内的 GET 可以看到前面 PUT 的修改

param(
    [string]$BaseUrl = "http://localhost:9081"
)

$ErrorActionPreference = "Stop"
$passed = 0
$failed = 0

function Write-TestResult($name, $success, $details = "") {
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

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  事务内可见性测试 (etcd 语义对齐)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 测试1: 事务内 PUT 后 GET 能看到新值
Write-Host "`n测试1: 事务内 PUT 后 GET 可见性" -ForegroundColor Yellow
$testKey = "txn-visibility-$(Get-Random)"
try {
    # 先设置初始值
    Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"old-value"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
    Start-Sleep -Milliseconds 200

    # 执行事务：PUT 新值，然后 GET 应该看到新值
    $body = @{
        compares = @()
        success = @(
            @{ type = "PUT"; key = $testKey; value = "new-value" }
            @{ type = "GET"; key = $testKey }
        )
        failure = @()
    } | ConvertTo-Json -Depth 10

    $r = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5

    # 验证 GET 返回的是新值
    $getResult = $r.results."1"
    $isVisible = ($getResult.value -eq "new-value")
    Write-TestResult "事务内 PUT 后 GET 可见性" $isVisible "GET 返回值: $($getResult.value)"
} catch {
    Write-TestResult "事务内 PUT 后 GET 可见性" $false $_.Exception.Message
}

# 测试2: 事务内 DELETE 后 GET 返回 null
Write-Host "`n测试2: 事务内 DELETE 后 GET 可见性" -ForegroundColor Yellow
$testKey = "txn-delete-$(Get-Random)"
try {
    # 先设置值
    Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"test-value"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
    Start-Sleep -Milliseconds 200

    # 执行事务：DELETE，然后 GET 应该返回 null
    $body = @{
        compares = @()
        success = @(
            @{ type = "DELETE"; key = $testKey }
            @{ type = "GET"; key = $testKey }
        )
        failure = @()
    } | ConvertTo-Json -Depth 10

    $r = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5

    # 验证 GET 返回 null
    $getResult = $r.results."1"
    $isNull = ($getResult.value -eq $null)
    Write-TestResult "事务内 DELETE 后 GET 返回 null" $isNull "GET 返回值: $($getResult.value)"
} catch {
    Write-TestResult "事务内 DELETE 后 GET 返回 null" $false $_.Exception.Message
}

# 测试3: MOD Revision 正确更新
Write-Host "`n测试3: MOD Revision 正确性" -ForegroundColor Yellow
$testKey = "txn-modrev-$(Get-Random)"
try {
    # 创建 key
    Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"v1"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
    Start-Sleep -Milliseconds 200

    # 获取初始 modRevision
    $r1 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5
    $modRev1 = $r1.modRevision
    Write-Host "  初始 modRevision: $modRev1" -ForegroundColor Gray

    # 修改 key
    Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"v2"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
    Start-Sleep -Milliseconds 200

    # 获取新的 modRevision
    $r2 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5
    $modRev2 = $r2.modRevision
    Write-Host "  修改后 modRevision: $modRev2" -ForegroundColor Gray

    # 验证 modRevision 增加了
    $isUpdated = ($modRev2 -gt $modRev1)
    Write-TestResult "MOD Revision 正确更新" $isUpdated "$modRev1 -> $modRev2"
} catch {
    Write-TestResult "MOD Revision 正确更新" $false $_.Exception.Message
}

# 测试4: 使用 MOD Revision 进行 CAS
Write-Host "`n测试4: 使用 MOD Revision 的 CAS" -ForegroundColor Yellow
$testKey = "txn-mod-cas-$(Get-Random)"
try {
    # 创建 key
    Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method PUT -Body '{"value":"v1"}' -ContentType "application/json" -TimeoutSec 5 | Out-Null
    Start-Sleep -Milliseconds 200

    # 获取 modRevision
    $r1 = Invoke-RestMethod -Uri "$BaseUrl/kv/$testKey" -Method GET -TimeoutSec 5
    $modRev = $r1.modRevision
    Write-Host "  当前 modRevision: $modRev" -ForegroundColor Gray

    # 使用正确的 modRevision 进行 CAS
    $body = @{
        compares = @(
            @{ target = "MOD"; key = $testKey; op = "EQUAL"; value = $modRev }
        )
        success = @(
            @{ type = "PUT"; key = $testKey; value = "v2" }
        )
        failure = @()
    } | ConvertTo-Json -Depth 10

    $r2 = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5
    $casSuccess = $r2.succeeded
    Write-TestResult "MOD Revision CAS 成功" $casSuccess

    # 使用错误的 modRevision 进行 CAS（应该失败）
    $body2 = @{
        compares = @(
            @{ target = "MOD"; key = $testKey; op = "EQUAL"; value = $modRev }
        )
        success = @(
            @{ type = "PUT"; key = $testKey; value = "v3" }
        )
        failure = @()
    } | ConvertTo-Json -Depth 10

    $r3 = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body2 -ContentType "application/json" -TimeoutSec 5
    $casFail = -not $r3.succeeded
    Write-TestResult "过期 MOD Revision CAS 失败" $casFail
} catch {
    Write-TestResult "MOD Revision CAS 测试" $false $_.Exception.Message
}

# 测试5: 多操作事务的原子性
Write-Host "`n测试5: 多操作事务原子性" -ForegroundColor Yellow
$key1 = "txn-atomic-1-$(Get-Random)"
$key2 = "txn-atomic-2-$(Get-Random)"
try {
    # 执行多 PUT 事务
    $body = @{
        compares = @()
        success = @(
            @{ type = "PUT"; key = $key1; value = "value1" }
            @{ type = "PUT"; key = $key2; value = "value2" }
        )
        failure = @()
    } | ConvertTo-Json -Depth 10

    $r = Invoke-RestMethod -Uri "$BaseUrl/txn" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 5

    Start-Sleep -Milliseconds 200

    # 验证两个 key 都被写入
    $v1 = (Invoke-RestMethod -Uri "$BaseUrl/kv/$key1" -Method GET -TimeoutSec 5).value
    $v2 = (Invoke-RestMethod -Uri "$BaseUrl/kv/$key2" -Method GET -TimeoutSec 5).value

    $bothWritten = ($v1 -eq "value1") -and ($v2 -eq "value2")
    Write-TestResult "多 PUT 事务原子性" $bothWritten "key1=$v1, key2=$v2"
} catch {
    Write-TestResult "多 PUT 事务原子性" $false $_.Exception.Message
}

# 总结
Write-Host "`n========================================" -ForegroundColor Blue
Write-Host "  测试结果汇总" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
$total = $passed + $failed
$passRate = [math]::Round(($passed / $total) * 100, 2)
Write-Host "总测试数: $total" -ForegroundColor White
Write-Host "通过: $passed" -ForegroundColor Green
Write-Host "失败: $failed" -ForegroundColor Red
Write-Host "通过率: $passRate%" -ForegroundColor $(if($passRate -ge 90){"Green"}elseif($passRate -ge 70){"Yellow"}else{"Red"})

if ($failed -eq 0) {
    Write-Host "`n所有测试通过！事务实现已对齐 etcd 语义。" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`n部分测试失败。" -ForegroundColor Red
    exit 1
}
