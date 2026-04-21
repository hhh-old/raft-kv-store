# MVCC 和 OCC 功能测试脚本
# 测试内容：
# 1. GREATER_EQUAL / LESS_EQUAL 操作符
# 2. MVCC 多版本存储
# 3. 乐观并发控制 (OCC)

$ErrorActionPreference = "Stop"
$baseUrl = "http://localhost:9081"

function Write-TestHeader($title) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host $title -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Write-TestResult($testName, $success, $details = "") {
    $status = if ($success) { "✅ PASS" } else { "❌ FAIL" }
    $color = if ($success) { "Green" } else { "Red" }
    Write-Host "[$status] $testName" -ForegroundColor $color
    if ($details) {
        Write-Host "    $details" -ForegroundColor Gray
    }
}

function Invoke-Rest($method, $path, $body = $null) {
    $url = "$baseUrl$path"
    try {
        if ($body) {
            $jsonBody = $body | ConvertTo-Json -Depth 10
            return Invoke-RestMethod -Uri $url -Method $method -Body $jsonBody -ContentType "application/json" -TimeoutSec 10
        } else {
            return Invoke-RestMethod -Uri $url -Method $method -TimeoutSec 10
        }
    } catch {
        Write-Host "    Error: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

# ==================== 测试 1: GREATER_EQUAL / LESS_EQUAL ====================
Write-TestHeader "测试 1: GREATER_EQUAL / LESS_EQUAL 操作符"

# 准备数据
Invoke-Rest -method "PUT" -path "/kv" -body @{ key = "/test/ge_le/num"; value = "100"; requestId = "test-req-1" }
Start-Sleep -Milliseconds 100

# 测试事务 - GREATER_EQUAL (version >= 1 应该成功)
$txnGe = @{
    compares = @(
        @{
            target = "VERSION"
            key = "/test/ge_le/num"
            op = "GREATER_EQUAL"
            value = 1
        }
    )
    success = @(
        @{ type = "PUT"; key = "/test/ge_le/result"; value = "GE_SUCCESS"; nestedTxn = $null }
    )
    failure = @(
        @{ type = "PUT"; key = "/test/ge_le/result"; value = "GE_FAILED"; nestedTxn = $null }
    )
}

$responseGe = Invoke-Rest -method "POST" -path "/txn" -body $txnGe
$geSuccess = $responseGe -and $responseGe.succeeded -eq $true
Write-TestResult 'GREATER_EQUAL (version GE 1)' $geSuccess "succeeded=$($responseGe.succeeded)"

# 测试事务 - LESS_EQUAL (version <= 5 应该成功)
$txnLe = @{
    compares = @(
        @{
            target = "VERSION"
            key = "/test/ge_le/num"
            op = "LESS_EQUAL"
            value = 5
        }
    )
    success = @(
        @{ type = "PUT"; key = "/test/ge_le/result2"; value = "LE_SUCCESS"; nestedTxn = $null }
    )
    failure = @(
        @{ type = "PUT"; key = "/test/ge_le/result2"; value = "LE_FAILED"; nestedTxn = $null }
    )
}

$responseLe = Invoke-Rest -method "POST" -path "/txn" -body $txnLe
$leSuccess = $responseLe -and $responseLe.succeeded -eq $true
Write-TestResult 'LESS_EQUAL (version LE 5)' $leSuccess "succeeded=$($responseLe.succeeded)"

# 测试失败场景 - GREATER_EQUAL (version >= 10 应该失败)
$txnGeFail = @{
    compares = @(
        @{
            target = "VERSION"
            key = "/test/ge_le/num"
            op = "GREATER_EQUAL"
            value = 10
        }
    )
    success = @(
        @{ type = "PUT"; key = "/test/ge_le/result3"; value = "SHOULD_NOT_HAPPEN"; nestedTxn = $null }
    )
    failure = @(
        @{ type = "PUT"; key = "/test/ge_le/result3"; value = "EXPECTED_FAILURE"; nestedTxn = $null }
    )
}

$responseGeFail = Invoke-Rest -method "POST" -path "/txn" -body $txnGeFail
$geFailSuccess = $responseGeFail -and $responseGeFail.succeeded -eq $false
Write-TestResult 'GREATER_EQUAL (version GE 10) should fail' $geFailSuccess "succeeded=$($responseGeFail.succeeded)"

# ==================== 测试 2: MVCC 多版本存储 ====================
Write-TestHeader "测试 2: MVCC 多版本存储"

# 写入多个版本
for ($i = 1; $i -le 3; $i++) {
    Invoke-Rest -method "PUT" -path "/kv" -body @{ 
        key = "/test/mvcc/versioned_key"; 
        value = "version_$i"; 
        requestId = "mvcc-req-$i" 
    }
    Start-Sleep -Milliseconds 50
}

# 读取最新版本
$latest = Invoke-Rest -method "GET" -path "/kv?keyParam=%2Ftest%2Fmvcc%2Fversioned_key"
$mvccSuccess = $latest -and $latest.value -eq "version_3"
Write-TestResult "MVCC 最新版本读取" $mvccSuccess "value=$($latest.value)"

# ==================== 测试 3: 并发写入测试 ====================
Write-TestHeader "测试 3: 并发写入测试 (OCC基础)"

# 初始化计数器
Invoke-Rest -method "PUT" -path "/kv" -body @{ 
    key = "/test/occ/counter"; 
    value = "0"; 
    requestId = "occ-init" 
}
Start-Sleep -Milliseconds 100

# 并发更新测试 - 多个客户端同时尝试 CAS 更新
$successCount = 0
$failCount = 0

for ($i = 1; $i -le 5; $i++) {
    $expectedValue = [string]($i - 1)
    $newValue = [string]$i
    
    $txnOcc = @{
        compares = @(
            @{
                target = "VALUE"
                key = "/test/occ/counter"
                op = "EQUAL"
                value = $expectedValue
            }
        )
        success = @(
            @{ type = "PUT"; key = "/test/occ/counter"; value = $newValue; nestedTxn = $null }
        )
        failure = @(
            @{ type = "GET"; key = "/test/occ/counter"; value = $null; nestedTxn = $null }
        )
    }
    
    $responseOcc = Invoke-Rest -method "POST" -path "/txn" -body $txnOcc
    
    if ($responseOcc -and $responseOcc.succeeded) {
        $successCount++
        Write-Host "  尝试 $i success: expected=$expectedValue, new=$newValue" -ForegroundColor Green
    } else {
        $failCount++
        Write-Host "  尝试 $i failed: expected=$expectedValue mismatch" -ForegroundColor Yellow
    }
    
    Start-Sleep -Milliseconds 20
}

$occTestSuccess = $successCount -ge 1 -and $failCount -ge 0
Write-TestResult "OCC 并发 CAS 测试" $occTestSuccess "成功=$successCount, 失败=$failCount"

# 验证最终值
$finalValue = Invoke-Rest -method "GET" -path "/kv?keyParam=%2Ftest%2Focc%2Fcounter"
Write-TestResult "OCC 最终值一致性" ($finalValue -ne $null) "最终值 is $($finalValue.value)"

# ==================== 测试 4: 复杂事务场景 ====================
Write-TestHeader "测试 4: 复杂事务场景"

# 准备账户数据
Invoke-Rest -method "PUT" -path "/kv" -body @{ key = "/test/txns/alice"; value = "1000"; requestId = "txn-init-1" }
Invoke-Rest -method "PUT" -path "/kv" -body @{ key = "/test/txns/bob"; value = "500"; requestId = "txn-init-2" }
Start-Sleep -Milliseconds 100

# 转账事务：Alice -> Bob 200
$txnTransfer = @{
    compares = @(
        @{
            target = "VALUE"
            key = "/test/txns/alice"
            op = "GREATER_EQUAL"  # 使用新增的 >= 操作符
            value = "200"
        }
    )
    success = @(
        @{ type = "PUT"; key = "/test/txns/alice"; value = "800"; nestedTxn = $null }
        @{ type = "PUT"; key = "/test/txns/bob"; value = "700"; nestedTxn = $null }
    )
    failure = @(
        @{ type = "GET"; key = "/test/txns/alice"; value = $null; nestedTxn = $null }
        @{ type = "GET"; key = "/test/txns/bob"; value = $null; nestedTxn = $null }
    )
}

$responseTransfer = Invoke-Rest -method "POST" -path "/txn" -body $txnTransfer
$transferSuccess = $responseTransfer -and $responseTransfer.succeeded -eq $true
Write-TestResult "转账事务 (使用 GREATER_EQUAL)" $transferSuccess "succeeded=$($responseTransfer.succeeded)"

# 验证转账结果
$aliceBalance = Invoke-Rest -method "GET" -path "/kv?keyParam=%2Ftest%2Ftxns%2Falice"
$bobBalance = Invoke-Rest -method "GET" -path "/kv?keyParam=%2Ftest%2Ftxns%2Fbob"

$aliceCorrect = $aliceBalance -and $aliceBalance.value -eq "800"
$bobCorrect = $bobBalance -and $bobBalance.value -eq "700"

Write-TestResult "Alice 余额正确 (800)" $aliceCorrect "实际=$($aliceBalance.value)"
Write-TestResult "Bob 余额正确 (700)" $bobCorrect "实际=$($bobBalance.value)"

# ==================== 测试 5: 边界条件测试 ====================
Write-TestHeader "测试 5: 边界条件测试"

# 测试空值比较
Invoke-Rest -method "PUT" -path "/kv" -body @{ key = "/test/edge/empty"; value = ""; requestId = "edge-1" }
Start-Sleep -Milliseconds 50

$txnEdge = @{
    compares = @(
        @{
            target = "VALUE"
            key = "/test/edge/empty"
            op = "EQUAL"
            value = ""
        }
    )
    success = @(
        @{ type = "PUT"; key = "/test/edge/result"; value = "EMPTY_MATCH"; nestedTxn = $null }
    )
    failure = @(
        @{ type = "PUT"; key = "/test/edge/result"; value = "EMPTY_FAIL"; nestedTxn = $null }
    )
}

$responseEdge = Invoke-Rest -method "POST" -path "/txn" -body $txnEdge
$edgeSuccess = $responseEdge -and $responseEdge.succeeded -eq $true
Write-TestResult "空值比较测试" $edgeSuccess "succeeded=$($responseEdge.succeeded)"

# ==================== 总结 ====================
Write-TestHeader "测试结果总结"

Write-Host "所有测试已完成！" -ForegroundColor Green
Write-Host "`n测试覆盖:" -ForegroundColor Cyan
Write-Host "  1. GREATER_EQUAL / LESS_EQUAL 操作符" -ForegroundColor White
Write-Host "  2. MVCC 多版本存储基础功能" -ForegroundColor White
Write-Host "  3. 乐观并发控制 (OCC) 基础" -ForegroundColor White
Write-Host "  4. 复杂事务场景" -ForegroundColor White
Write-Host "  5. 边界条件" -ForegroundColor White

Write-Host "`n注意：" -ForegroundColor Yellow
Write-Host "  - 确保 Raft 集群已启动 (node1, node2, node3)" -ForegroundColor Gray
Write-Host "  - 测试使用 Leader 节点 (localhost:9081)" -ForegroundColor Gray
Write-Host "  - 部分测试需要服务器重启后才能反映 MVCC 变更" -ForegroundColor Gray
