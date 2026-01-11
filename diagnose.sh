#!/bin/bash

# Shadow-GRE 诊断脚本
# 用于快速检查和诊断问题

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Shadow-GRE 诊断工具${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查 1: 编译状态
echo -e "${YELLOW}[1] 检查编译状态...${NC}"
if [ -f "./shadow-gre" ]; then
    echo -e "  ${GREEN}✓${NC} shadow-gre 二进制文件存在"
    FILE_SIZE=$(ls -lh shadow-gre | awk '{print $5}')
    echo -e "    文件大小: $FILE_SIZE"

    # 检查是否是最新的
    BINARY_TIME=$(stat -f %m shadow-gre 2>/dev/null || stat -c %Y shadow-gre 2>/dev/null)
    GO_FILES_TIME=$(find . -name "*.go" -exec stat -f %m {} \; 2>/dev/null | sort -n | tail -1)
    if [ -z "$GO_FILES_TIME" ]; then
        GO_FILES_TIME=$(find . -name "*.go" -exec stat -c %Y {} \; 2>/dev/null | sort -n | tail -1)
    fi

    if [ "$BINARY_TIME" -lt "$GO_FILES_TIME" ]; then
        echo -e "  ${YELLOW}⚠${NC}  警告: 源代码比二进制文件新，建议重新编译"
        echo -e "    运行: go build -o shadow-gre ./cmd/shadow-gre"
    fi
else
    echo -e "  ${RED}✗${NC} shadow-gre 二进制文件不存在"
    echo -e "    运行: go build -o shadow-gre ./cmd/shadow-gre"
fi
echo ""

# 检查 2: 依赖检查
echo -e "${YELLOW}[2] 检查依赖...${NC}"
if command -v go &> /dev/null; then
    GO_VERSION=$(go version)
    echo -e "  ${GREEN}✓${NC} Go: $GO_VERSION"
else
    echo -e "  ${RED}✗${NC} Go 未安装"
fi

if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "  ${GREEN}✓${NC} Python3: $PYTHON_VERSION"
else
    echo -e "  ${YELLOW}⚠${NC}  Python3 未安装 (测试脚本需要)"
fi

if command -v curl &> /dev/null; then
    echo -e "  ${GREEN}✓${NC} curl 已安装"
else
    echo -e "  ${RED}✗${NC} curl 未安装"
fi

if command -v nc &> /dev/null; then
    echo -e "  ${GREEN}✓${NC} netcat 已安装"
else
    echo -e "  ${YELLOW}⚠${NC}  netcat 未安装"
fi
echo ""

# 检查 3: 权限检查
echo -e "${YELLOW}[3] 检查权限...${NC}"
if [ "$EUID" -eq 0 ]; then
    echo -e "  ${GREEN}✓${NC} 以 root 权限运行"
else
    echo -e "  ${RED}✗${NC} 需要 root 权限 (创建 raw socket)"
    echo -e "    运行: sudo $0"
fi
echo ""

# 检查 4: 进程检查
echo -e "${YELLOW}[4] 检查运行中的进程...${NC}"
RUNNING_PROCESSES=$(ps aux | grep -E "shadow-gre|test_server" | grep -v grep)
if [ -z "$RUNNING_PROCESSES" ]; then
    echo -e "  ${BLUE}ℹ${NC}  没有运行中的 shadow-gre 进程"
else
    echo -e "  ${YELLOW}⚠${NC}  发现运行中的进程:"
    echo "$RUNNING_PROCESSES" | while read line; do
        echo -e "    $line"
    done
    echo ""
    echo -e "  ${YELLOW}建议:${NC} 清理这些进程后再测试"
    echo -e "    运行: sudo pkill -f shadow-gre"
fi
echo ""

# 检查 5: 端口检查
echo -e "${YELLOW}[5] 检查端口占用...${NC}"
PORTS=(1080 8388)
for PORT in "${PORTS[@]}"; do
    if lsof -i :$PORT &> /dev/null; then
        PROCESS=$(lsof -i :$PORT | tail -n 1 | awk '{print $1, $2}')
        echo -e "  ${YELLOW}⚠${NC}  端口 $PORT 被占用: $PROCESS"
    else
        echo -e "  ${GREEN}✓${NC} 端口 $PORT 可用"
    fi
done
echo ""

# 检查 6: 网络检查
echo -e "${YELLOW}[6] 检查网络配置...${NC}"

# 检查 loopback 接口
if ip addr show lo &> /dev/null 2>&1; then
    LO_STATUS=$(ip addr show lo | grep "state UP" &> /dev/null && echo "UP" || echo "DOWN")
    if [ "$LO_STATUS" = "UP" ]; then
        echo -e "  ${GREEN}✓${NC} loopback 接口状态: UP"
    else
        echo -e "  ${RED}✗${NC} loopback 接口状态: DOWN"
    fi
elif ifconfig lo0 &> /dev/null 2>&1; then
    # macOS
    echo -e "  ${GREEN}✓${NC} loopback 接口 (lo0) 存在"
fi

# 测试 127.0.0.1 连通性
if ping -c 1 -W 1 127.0.0.1 &> /dev/null; then
    echo -e "  ${GREEN}✓${NC} 127.0.0.1 可达"
else
    echo -e "  ${RED}✗${NC} 127.0.0.1 不可达"
fi
echo ""

# 检查 7: 代码完整性检查
echo -e "${YELLOW}[7] 检查代码完整性...${NC}"

REQUIRED_FILES=(
    "pkg/gre/gre.go"
    "pkg/tunnel/stream.go"
    "pkg/transport/raw.go"
    "pkg/transport/server.go"
    "pkg/shadowgre/client.go"
    "pkg/shadowgre/server.go"
    "cmd/shadow-gre/main.go"
)

ALL_FILES_EXIST=true
for FILE in "${REQUIRED_FILES[@]}"; do
    if [ -f "$FILE" ]; then
        echo -e "  ${GREEN}✓${NC} $FILE"
    else
        echo -e "  ${RED}✗${NC} $FILE (缺失)"
        ALL_FILES_EXIST=false
    fi
done

if [ "$ALL_FILES_EXIST" = false ]; then
    echo -e "  ${RED}错误:${NC} 某些必需文件缺失"
fi
echo ""

# 检查 8: 最近的修改
echo -e "${YELLOW}[8] 最近修改的文件...${NC}"
echo -e "  最近 5 个修改的 .go 文件:"
find . -name "*.go" -type f -print0 | xargs -0 ls -lt | head -5 | while read line; do
    FILE=$(echo "$line" | awk '{print $9}')
    TIME=$(echo "$line" | awk '{print $6, $7, $8}')
    echo -e "    $TIME - $FILE"
done
echo ""

# 检查 9: GRE 协议支持
echo -e "${YELLOW}[9] 检查 GRE 协议支持...${NC}"

if [ -f "/proc/net/protocols" ]; then
    if grep -q "GRE" /proc/net/protocols 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} 内核支持 GRE 协议"
    else
        echo -e "  ${YELLOW}⚠${NC}  可能不支持 GRE (检查 /proc/net/protocols)"
    fi
else
    # macOS
    echo -e "  ${BLUE}ℹ${NC}  macOS 系统，跳过内核协议检查"
fi
echo ""

# 建议
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  诊断总结${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if [ ! -f "./shadow-gre" ]; then
    echo -e "${RED}1. 先编译项目:${NC}"
    echo -e "   go build -o shadow-gre ./cmd/shadow-gre"
    echo ""
fi

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}2. 使用 root 权限运行测试:${NC}"
    echo -e "   sudo ./test-shadow-gre.sh"
    echo ""
fi

if [ ! -z "$RUNNING_PROCESSES" ]; then
    echo -e "${YELLOW}3. 清理运行中的进程:${NC}"
    echo -e "   sudo pkill -f shadow-gre"
    echo -e "   sudo pkill -f test_server"
    echo ""
fi

echo -e "${GREEN}准备好后运行测试:${NC}"
echo -e "   sudo ./test-shadow-gre.sh"
echo ""

echo -e "${YELLOW}或者手动测试:${NC}"
echo -e "   1. 启动 backend:  python3 -m http.server 8388"
echo -e "   2. 启动 server:   sudo ./shadow-gre -mode server -listen 127.0.0.1 -backend 127.0.0.1:8388 -password test123"
echo -e "   3. 启动 client:   sudo ./shadow-gre -mode client -listen 127.0.0.1:1080 -local 127.0.0.1 -server 127.0.0.1 -password test123"
echo -e "   4. 测试连接:      curl http://127.0.0.1:1080/"
echo ""
