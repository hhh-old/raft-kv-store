@echo off
REM Raft KV Client 测试脚本

echo ====================================
echo Raft KV Client 测试
echo ====================================
echo.

REM 检查服务端是否运行
echo [1] 检查服务端状态...
curl -s http://localhost:9081/kv/health >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ 服务端正在运行
) else (
    echo ❌ 服务端未运行，请先启动服务端
    echo.
    echo 启动命令:
    echo   cd d:\project\Claudecode\raft-kv-store
    echo   $env:SPRING_PROFILES_ACTIVE="node1"
    echo   mvn spring-boot:run
    echo.
    pause
    exit /b 1
)
echo.

REM 编译客户端
echo [2] 编译客户端...
cd /d "%~dp0"
call mvn clean compile -q
if %errorlevel% equ 0 (
    echo ✅ 编译成功
) else (
    echo ❌ 编译失败
    pause
    exit /b 1
)
echo.

REM 运行示例
echo [3] 运行客户端示例...
echo.
call mvn exec:java -Dexec.mainClass="com.raftkv.client.example.ClientExample" -q

echo.
echo ====================================
echo 测试完成
echo ====================================
pause
