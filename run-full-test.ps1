# Raft KV Store 完整集成测试
# 1. 清理数据 2. 启动集群 3. 运行测试 4. 关闭集群

param(
    [switch]$KeepCluster = $false
)

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Blue
Write-Host "  Raft KV Store 完整集成测试" -ForegroundColor Blue
Write-Host "============================================" -ForegroundColor Blue
Write-Host ""

# Step 1: 停止现有集群
Write-Host "[Step 1/5] 停止现有集群..." -NoNewline
try {
    Stop-Process -Name java -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Write-Host " OK" -ForegroundColor Green
} catch {
    Write-Host " (no running processes)" -ForegroundColor Yellow
}

# Step 2: 清理数据目录
Write-Host "[Step 2/5] 清理数据目录..." -NoNewline
try {
    $dataDirs = @("data\node1", "data\node2", "data\node3")
    foreach ($dir in $dataDirs) {
        if (Test-Path $dir) {
            Remove-Item -Path "$dir\*" -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    Write-Host " OK" -ForegroundColor Green
} catch {
    Write-Host " Warning: $_" -ForegroundColor Yellow
}

# Step 3: 编译项目
Write-Host "[Step 3/5] 编译项目..." -NoNewline
try {
    $compileOutput = mvn clean package -DskipTests -q 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "编译失败"
    }
    Write-Host " OK" -ForegroundColor Green
} catch {
    Write-Host " FAILED" -ForegroundColor Red
    Write-Host $compileOutput -ForegroundColor Red
    exit 1
}

# Step 4: 启动集群
Write-Host "[Step 4/5] 启动集群..." -ForegroundColor Cyan

$jvmArgs = @(
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

$jarPath = "raft-kv-server\target\raft-kv-server-1.0.0.jar"

# 启动节点1
Write-Host "  Starting Node 1..." -NoNewline
$node1 = Start-Process -FilePath "java" -ArgumentList ($jvmArgs + "-jar", $jarPath, "--spring.profiles.active=node1") -PassThru -WindowStyle Hidden
Write-Host " PID:$($node1.Id)" -ForegroundColor Gray
Start-Sleep -Seconds 5

# 启动节点2
Write-Host "  Starting Node 2..." -NoNewline
$node2 = Start-Process -FilePath "java" -ArgumentList ($jvmArgs + "-jar", $jarPath, "--spring.profiles.active=node2") -PassThru -WindowStyle Hidden
Write-Host " PID:$($node2.Id)" -ForegroundColor Gray
Start-Sleep -Seconds 3

# 启动节点3
Write-Host "  Starting Node 3..." -NoNewline
$node3 = Start-Process -FilePath "java" -ArgumentList ($jvmArgs + "-jar", $jarPath, "--spring.profiles.active=node3") -PassThru -WindowStyle Hidden
Write-Host " PID:$($node3.Id)" -ForegroundColor Gray

Write-Host "`n  等待集群选举..." -NoNewline
Start-Sleep -Seconds 15

# 检查集群状态
$clusterReady = $false
for ($i = 0; $i -lt 10; $i++) {
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:9081/kv/stats" -Method GET -TimeoutSec 2
        if ($response.role -eq "LEADER") {
            Write-Host " OK (Leader elected)" -ForegroundColor Green
            $clusterReady = $true
            break
        }
    } catch {
        Start-Sleep -Seconds 1
    }
}

if (-not $clusterReady) {
    Write-Host " FAILED (no leader)" -ForegroundColor Red
    Stop-Process -Name java -Force -ErrorAction SilentlyContinue
    exit 1
}

# Step 5: 运行测试
Write-Host "`n[Step 5/5] 运行功能测试..." -ForegroundColor Cyan
Write-Host ""

$testResult = & "./test-all.ps1" -BaseUrl "http://localhost:9081"

# 清理
if (-not $KeepCluster) {
    Write-Host "`n[Cleanup] 关闭集群..." -NoNewline
    Stop-Process -Name java -Force -ErrorAction SilentlyContinue
    Write-Host " OK" -ForegroundColor Green
} else {
    Write-Host "`n[Info] 集群保持运行，HTTP端口: 9081, 9082, 9083" -ForegroundColor Cyan
}

Write-Host "`n============================================" -ForegroundColor Blue
if ($LASTEXITCODE -eq 0) {
    Write-Host "  所有测试通过!" -ForegroundColor Green
} else {
    Write-Host "  部分测试失败!" -ForegroundColor Red
}
Write-Host "============================================" -ForegroundColor Blue

exit $LASTEXITCODE
