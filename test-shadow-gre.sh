#!/bin/bash

# Shadow-GRE 全自动测试脚本
# 用于测试 TCP over GRE 是否正常工作

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
PASSWORD="test-password-12345"
BACKEND_PORT=8388
CLIENT_PORT=1080
LOCAL_IP="127.0.0.2"
SERVER_IP="127.0.0.3"

# PID 文件
BACKEND_PID=""
SERVER_PID=""
CLIENT_PID=""
LOG_DIR="./test-logs"

# 清理函数
cleanup() {
    echo -e "\n${YELLOW}清理进程...${NC}"

    if [ ! -z "$CLIENT_PID" ]; then
        echo "停止 Client (PID: $CLIENT_PID)"
        sudo kill -TERM $CLIENT_PID 2>/dev/null || true
    fi

    if [ ! -z "$SERVER_PID" ]; then
        echo "停止 Server (PID: $SERVER_PID)"
        sudo kill -TERM $SERVER_PID 2>/dev/null || true
    fi

    if [ ! -z "$BACKEND_PID" ]; then
        echo "停止 Backend (PID: $BACKEND_PID)"
        kill -TERM $BACKEND_PID 2>/dev/null || true
    fi

    # 等待进程退出
    sleep 2

    # 强制清理
    pkill -f "shadow-gre.*-mode server" 2>/dev/null || true
    pkill -f "shadow-gre.*-mode client" 2>/dev/null || true
    pkill -f "nc.*-l.*$BACKEND_PORT" 2>/dev/null || true

    # Remove IP aliases (macOS specific)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Removing IP aliases..."
        sudo ifconfig lo0 -alias $LOCAL_IP 2>/dev/null || true
        sudo ifconfig lo0 -alias $SERVER_IP 2>/dev/null || true
    fi

    echo -e "${GREEN}清理完成${NC}"
}

# 错误处理
error_exit() {
    echo -e "${RED}错误: $1${NC}" >&2
    cleanup
    exit 1
}

# 捕获退出信号
trap cleanup EXIT INT TERM

# 检查是否是 root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}此脚本需要 root 权限运行（因为需要创建 raw socket）${NC}"
    echo "请使用: sudo $0"
    exit 1
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Shadow-GRE 全自动测试脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 创建日志目录
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log

# Setup IP aliases for macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${YELLOW}设置 IP 别名 (macOS)...${NC}"
    sudo ifconfig lo0 alias $LOCAL_IP up
    sudo ifconfig lo0 alias $SERVER_IP up
fi

# 步骤 1: 构建项目
echo -e "${YELLOW}[1/7] 构建项目...${NC}"
go build -o shadow-gre ./cmd/shadow-gre || error_exit "构建失败"
echo -e "${GREEN}✓ 构建成功${NC}"
echo ""

# 步骤 2: 启动 Backend 服务器（简单的 HTTP 服务器）
echo -e "${YELLOW}[2/7] 启动 Backend 测试服务器 (端口 $BACKEND_PORT)...${NC}"

# 创建一个简单的 Python HTTP 服务器
cat > "$LOG_DIR/test_server.py" << 'EOF'
#!/usr/bin/env python3
import http.server
import socketserver
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8388

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write("[BACKEND] %s - - [%s] %s\n" %
                         (self.address_string(),
                          self.log_date_time_string(),
                          format%args))

    def do_GET(self):
        if self.path == '/test':
            response = b"Hello from backend! " * 100  # 2KB 响应
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header('Content-Length', str(len(response)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(response)
        elif self.path == '/large':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            # 发送 1MB 数据
            chunk = b"X" * 1024
            for _ in range(1024):
                self.wfile.write(chunk)
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK\n")

Handler = CustomHandler
class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

with ThreadingTCPServer(("", PORT), Handler) as httpd:
    print(f"Backend server running on port {PORT}", file=sys.stderr)
    httpd.serve_forever()
EOF

chmod +x "$LOG_DIR/test_server.py"

python3 "$LOG_DIR/test_server.py" $BACKEND_PORT > "$LOG_DIR/backend.log" 2>&1 &
BACKEND_PID=$!

sleep 1

# 验证 backend 是否启动
if ! kill -0 $BACKEND_PID 2>/dev/null; then
    error_exit "Backend 服务器启动失败，查看日志: cat $LOG_DIR/backend.log"
fi

if ! nc -z 127.0.0.1 $BACKEND_PORT 2>/dev/null; then
    error_exit "Backend 服务器端口 $BACKEND_PORT 无法访问"
fi

echo -e "${GREEN}✓ Backend 服务器已启动 (PID: $BACKEND_PID)${NC}"
echo ""

# 步骤 3: 启动 Shadow-GRE Server
echo -e "${YELLOW}[3/7] 启动 Shadow-GRE Server...${NC}"

./shadow-gre \
    -mode server \
    -local "$SERVER_IP" \
    -backend "127.0.0.1:$BACKEND_PORT" \
    -password "$PASSWORD" \
    > "$LOG_DIR/server.log" 2>&1 &
SERVER_PID=$!

sleep 2

# 验证 server 是否启动
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${RED}Server 启动失败，查看日志:${NC}"
    cat "$LOG_DIR/server.log"
    error_exit "Server 进程已退出"
fi

echo -e "${GREEN}✓ Shadow-GRE Server 已启动 (PID: $SERVER_PID)${NC}"
echo ""

# 步骤 4: 启动 Shadow-GRE Client
echo -e "${YELLOW}[4/7] 启动 Shadow-GRE Client...${NC}"

./shadow-gre \
    -mode client \
    -listen "127.0.0.1:$CLIENT_PORT" \
    -local "$LOCAL_IP" \
    -remote "$SERVER_IP" \
    -password "$PASSWORD" \
    > "$LOG_DIR/client.log" 2>&1 &
CLIENT_PID=$!

sleep 2

# 验证 client 是否启动
if ! kill -0 $CLIENT_PID 2>/dev/null; then
    echo -e "${RED}Client 启动失败，查看日志:${NC}"
    cat "$LOG_DIR/client.log"
    error_exit "Client 进程已退出"
fi

# 验证 client 端口是否可访问
if ! nc -z 127.0.0.1 $CLIENT_PORT 2>/dev/null; then
    error_exit "Client 端口 $CLIENT_PORT 无法访问"
fi

echo -e "${GREEN}✓ Shadow-GRE Client 已启动 (PID: $CLIENT_PID)${NC}"
echo ""

# 步骤 5: 测试连接
echo -e "${YELLOW}[5/7] 测试基本连接...${NC}"

# 等待所有组件稳定
sleep 2

# 测试 1: 直接访问 backend (基准测试)
echo -e "  ${BLUE}→${NC} 测试直接访问 backend..."
if ! curl -s --max-time 5 "http://127.0.0.1:$BACKEND_PORT/test" > /dev/null; then
    error_exit "无法直接访问 backend，backend 可能有问题"
fi
echo -e "    ${GREEN}✓ Backend 直接访问正常${NC}"

# 测试 2: 通过 GRE 隧道访问
echo -e "  ${BLUE}→${NC} 测试通过 GRE 隧道访问..."
TUNNEL_RESPONSE=$(curl -s --max-time 10 "http://127.0.0.1:$CLIENT_PORT/test" 2>&1)
CURL_EXIT=$?

if [ $CURL_EXIT -ne 0 ]; then
    echo -e "${RED}✗ 隧道访问失败 (curl exit code: $CURL_EXIT)${NC}"
    echo -e "${RED}响应: $TUNNEL_RESPONSE${NC}"
    echo ""
    echo -e "${YELLOW}查看日志:${NC}"
    echo -e "${YELLOW}=== Client Log ===${NC}"
    tail -50 "$LOG_DIR/client.log"
    echo ""
    echo -e "${YELLOW}=== Server Log ===${NC}"
    tail -50 "$LOG_DIR/server.log"
    echo ""
    echo -e "${YELLOW}=== Backend Log ===${NC}"
    tail -20 "$LOG_DIR/backend.log"
    error_exit "GRE 隧道测试失败"
fi

# 验证响应内容
if [[ ! "$TUNNEL_RESPONSE" =~ "Hello from backend!" ]]; then
    echo -e "${RED}✗ 响应内容不正确${NC}"
    echo "期望: 'Hello from backend!'"
    echo "实际: $TUNNEL_RESPONSE"
    error_exit "响应验证失败"
fi

echo -e "    ${GREEN}✓ GRE 隧道访问正常${NC}"
echo -e "${GREEN}✓ 基本连接测试通过${NC}"
echo ""

# 步骤 6: 性能测试
echo -e "${YELLOW}[6/7] 性能测试...${NC}"

# 测试小包性能
echo -e "  ${BLUE}→${NC} 测试小包传输 (10 次请求)..."
SMALL_START=$(date +%s%N)
for i in {1..10}; do
    curl -s --max-time 5 "http://127.0.0.1:$CLIENT_PORT/test" > /dev/null || error_exit "小包测试失败 (请求 $i)"
done
SMALL_END=$(date +%s%N)
SMALL_TIME=$(( ($SMALL_END - $SMALL_START) / 1000000 ))
echo -e "    ${GREEN}✓ 10 次请求完成，总耗时: ${SMALL_TIME}ms，平均: $((SMALL_TIME/10))ms/req${NC}"

# 测试大包性能
echo -e "  ${BLUE}→${NC} 测试大包传输 (1MB x 3)..."
LARGE_START=$(date +%s%N)
for i in {1..3}; do
    curl -s --max-time 30 "http://127.0.0.1:$CLIENT_PORT/large" > /dev/null || error_exit "大包测试失败 (请求 $i)"
done
LARGE_END=$(date +%s%N)
LARGE_TIME=$(( ($LARGE_END - $LARGE_START) / 1000000 ))
THROUGHPUT=$(echo "scale=2; 3 * 1024 / ($LARGE_TIME / 1000)" | bc)
echo -e "    ${GREEN}✓ 3MB 传输完成，总耗时: ${LARGE_TIME}ms，吞吐量: ${THROUGHPUT} KB/s${NC}"

echo -e "${GREEN}✓ 性能测试完成${NC}"
echo ""

# 步骤 7: 并发测试
echo -e "${YELLOW}[7/7] 并发连接测试...${NC}"

# 并发测试函数 - 正确检测所有请求的成功/失败
run_concurrent_test() {
    local count=$1
    local timeout=$2
    local pids=()
    local failed=0

    for i in $(seq 1 $count); do
        curl -s --max-time $timeout "http://127.0.0.1:$CLIENT_PORT/test" > "$LOG_DIR/curl_$i.out" 2>&1 &
        pids+=($!)
    done

    # 等待所有请求完成并检查每个的退出状态
    for pid in "${pids[@]}"; do
        if ! wait $pid; then
            ((failed++))
        fi
    done

    # 验证响应内容
    for i in $(seq 1 $count); do
        if ! grep -q "Hello from backend!" "$LOG_DIR/curl_$i.out" 2>/dev/null; then
            ((failed++))
        fi
        rm -f "$LOG_DIR/curl_$i.out"
    done

    return $failed
}

echo -e "  ${BLUE}→${NC} 测试并发 4 个请求..."
CONCURRENT_START=$(date +%s%N)

if ! run_concurrent_test 4 10; then
    echo -e "    ${RED}✗ 4 个并发请求部分失败${NC}"
    echo -e "${YELLOW}=== Client Log (last 50 lines) ===${NC}"
    tail -50 "$LOG_DIR/client.log"
    echo ""
    echo -e "${YELLOW}=== Server Log (last 50 lines) ===${NC}"
    tail -50 "$LOG_DIR/server.log"
    error_exit "4 并发测试失败"
fi

CONCURRENT_END=$(date +%s%N)
CONCURRENT_TIME=$(( ($CONCURRENT_END - $CONCURRENT_START) / 1000000 ))
echo -e "    ${GREEN}✓ 4 个并发请求完成，总耗时: ${CONCURRENT_TIME}ms${NC}"

# 测试 10 并发
echo -e "  ${BLUE}→${NC} 测试并发 10 个请求..."
CONCURRENT10_START=$(date +%s%N)

if ! run_concurrent_test 10 15; then
    echo -e "    ${RED}✗ 10 个并发请求部分失败${NC}"
    echo -e "${YELLOW}查看日志:${NC}"
    echo -e "${YELLOW}=== Client Log (last 100 lines) ===${NC}"
    tail -100 "$LOG_DIR/client.log"
    echo ""
    echo -e "${YELLOW}=== Server Log (last 100 lines) ===${NC}"
    tail -100 "$LOG_DIR/server.log"
    error_exit "10 并发测试失败"
fi

CONCURRENT10_END=$(date +%s%N)
CONCURRENT10_TIME=$(( ($CONCURRENT10_END - $CONCURRENT10_START) / 1000000 ))
echo -e "    ${GREEN}✓ 10 个并发请求完成，总耗时: ${CONCURRENT10_TIME}ms${NC}"

# 测试 20 并发
echo -e "  ${BLUE}→${NC} 测试并发 20 个请求..."
CONCURRENT20_START=$(date +%s%N)

if ! run_concurrent_test 20 20; then
    echo -e "    ${RED}✗ 20 个并发请求部分失败${NC}"
    echo -e "${YELLOW}查看日志:${NC}"
    echo -e "${YELLOW}=== Client Log (last 100 lines) ===${NC}"
    tail -100 "$LOG_DIR/client.log"
    echo ""
    echo -e "${YELLOW}=== Server Log (last 100 lines) ===${NC}"
    tail -100 "$LOG_DIR/server.log"
    error_exit "20 并发测试失败"
fi

CONCURRENT20_END=$(date +%s%N)
CONCURRENT20_TIME=$(( ($CONCURRENT20_END - $CONCURRENT20_START) / 1000000 ))
echo -e "    ${GREEN}✓ 20 个并发请求完成，总耗时: ${CONCURRENT20_TIME}ms${NC}"

echo -e "${GREEN}✓ 并发测试完成${NC}"
echo ""

# 显示统计信息
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  测试结果统计${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 从日志中提取统计
echo -e "${YELLOW}Client 日志片段:${NC}"
tail -20 "$LOG_DIR/client.log" | grep -E "(New connection|Connection closed|Stream)" || echo "  (无相关日志)"
echo ""

echo -e "${YELLOW}Server 日志片段:${NC}"
tail -20 "$LOG_DIR/server.log" | grep -E "(Stream|forwarding|closed)" || echo "  (无相关日志)"
echo ""

# 最终结果
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  所有测试通过！✓${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${GREEN}总结:${NC}"
echo -e "  • 基本连接: ${GREEN}通过${NC}"
echo -e "  • 小包传输: ${GREEN}通过${NC} (平均 $((SMALL_TIME/10))ms/req)"
echo -e "  • 大包传输: ${GREEN}通过${NC} (${THROUGHPUT} KB/s)"
echo -e "  • 并发连接: ${GREEN}通过${NC} (4 个并发，${CONCURRENT_TIME}ms)"
echo -e "  • 高并发:   ${GREEN}通过${NC} (10 个并发，${CONCURRENT10_TIME}ms)"
echo -e "  • 更高并发: ${GREEN}通过${NC} (20 个并发，${CONCURRENT20_TIME}ms)"
echo ""
echo -e "${YELLOW}日志文件位置:${NC}"
echo -e "  • Client:  $LOG_DIR/client.log"
echo -e "  • Server:  $LOG_DIR/server.log"
echo -e "  • Backend: $LOG_DIR/backend.log"
echo ""

# 清理会在 trap 中自动执行
exit 0
