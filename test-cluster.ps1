# 启动 Raft KV Store 集群测试
$jarPath = "raft-kv-server\target\raft-kv-server-1.0.0.jar"

# 启动节点1
$node1 = Start-Process -FilePath "java" -ArgumentList "-jar", $jarPath, "--spring.profiles.active=node1" -PassThru -WindowStyle Hidden
Write-Host "Node1 started, PID: $($node1.Id)"
Start-Sleep -Seconds 5

# 启动节点2
$node2 = Start-Process -FilePath "java" -ArgumentList "-jar", $jarPath, "--spring.profiles.active=node2" -PassThru -WindowStyle Hidden
Write-Host "Node2 started, PID: $($node2.Id)"
Start-Sleep -Seconds 3

# 启动节点3
$node3 = Start-Process -FilePath "java" -ArgumentList "-jar", $jarPath, "--spring.profiles.active=node3" -PassThru -WindowStyle Hidden
Write-Host "Node3 started, PID: $($node3.Id)"

Write-Host "All nodes started!"
Start-Sleep -Seconds 10

# 检查健康状态
try {
    $response = Invoke-RestMethod -Uri "http://localhost:8081/raft/stats" -Method GET -TimeoutSec 5
    Write-Host "Node1 is healthy: $($response | ConvertTo-Json -Depth 2)"
} catch {
    Write-Host "Node1 health check failed: $_"
}
