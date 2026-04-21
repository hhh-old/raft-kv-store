# 启动 Raft KV Store 集群
$jvmArgs = @(
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

$jarPath = "raft-kv-server\target\raft-kv-server-1.0.0.jar"

Write-Host "Starting Raft KV Store Cluster..."
Write-Host "=================================="

# 启动节点1
Write-Host "Starting Node 1..."
$node1 = Start-Process -FilePath "java" -ArgumentList ($jvmArgs + "-jar", $jarPath, "--spring.profiles.active=node1") -PassThru -WindowStyle Hidden
Write-Host "Node1 PID: $($node1.Id)"
Start-Sleep -Seconds 5

# 启动节点2
Write-Host "Starting Node 2..."
$node2 = Start-Process -FilePath "java" -ArgumentList ($jvmArgs + "-jar", $jarPath, "--spring.profiles.active=node2") -PassThru -WindowStyle Hidden
Write-Host "Node2 PID: $($node2.Id)"
Start-Sleep -Seconds 3

# 启动节点3
Write-Host "Starting Node 3..."
$node3 = Start-Process -FilePath "java" -ArgumentList ($jvmArgs + "-jar", $jarPath, "--spring.profiles.active=node3") -PassThru -WindowStyle Hidden
Write-Host "Node3 PID: $($node3.Id)"

Write-Host ""
Write-Host "All nodes started! Waiting for cluster to stabilize..."
Start-Sleep -Seconds 10

# 检查集群状态
Write-Host ""
Write-Host "Checking cluster status..."
try {
    $response = Invoke-RestMethod -Uri "http://localhost:8081/raft/stats" -Method GET -TimeoutSec 5
    Write-Host "Node1 Status: $($response | ConvertTo-Json -Depth 2)"
} catch {
    Write-Host "Node1 not ready yet: $_"
}

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8082/raft/stats" -Method GET -TimeoutSec 5
    Write-Host "Node2 Status: $($response | ConvertTo-Json -Depth 2)"
} catch {
    Write-Host "Node2 not ready yet: $_"
}

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8083/raft/stats" -Method GET -TimeoutSec 5
    Write-Host "Node3 Status: $($response | ConvertTo-Json -Depth 2)"
} catch {
    Write-Host "Node3 not ready yet: $_"
}

Write-Host ""
Write-Host "Cluster startup complete!"
Write-Host "HTTP Endpoints: http://localhost:9081, http://localhost:9082, http://localhost:9083"
