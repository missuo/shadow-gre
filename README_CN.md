# Shadow GRE

[![Release](https://img.shields.io/github/v/release/missuo/shadow-gre)](https://github.com/missuo/shadow-gre/releases)
[![License](https://img.shields.io/github/license/missuo/shadow-gre)](LICENSE)

[English](README.md) | 中文

TCP over GRE 隧道 - 将 TCP 流量封装到 GRE 协议（IP 协议 47）中进行可靠传输。

## 架构

```
┌─────────────────┐                                    ┌─────────────────┐
│   应用程序       │                                    │   后端服务       │
│  (如浏览器)      │                                    │ (如 ss-server)  │
└────────┬────────┘                                    └────────▲────────┘
         │ TCP                                                  │ TCP
         ▼                                                      │
┌─────────────────┐         GRE 协议 47               ┌─────────┴────────┐
│  shadow-gre     │ ◄─────────────────────────────────► │  shadow-gre     │
│  (客户端模式)    │         原始 IP 套接字             │  (服务端模式)    │
└─────────────────┘                                    └──────────────────┘
```

## 特性

- 使用真正的 GRE 协议（IP 协议 47）
- **可靠传输层**，支持重传和 SACK
- 基于 RTT 测量的自适应 RTO（RFC 6298）
- 支持多连接复用
- 通过 GRE Key 字段进行简单认证

## 安装

### 下载预编译二进制文件

从 [GitHub Releases](https://github.com/missuo/shadow-gre/releases) 下载最新版本。

可用的二进制文件：
- `shadow-gre-linux-amd64` - Linux x86_64
- `shadow-gre-linux-arm64` - Linux ARM64
- `shadow-gre-linux-armv7` - Linux ARMv7
- `shadow-gre-darwin-amd64` - macOS Intel
- `shadow-gre-darwin-arm64` - macOS Apple Silicon
- `shadow-gre-freebsd-amd64` - FreeBSD x86_64

### 从源码构建

要求：
- Go 1.21+
- Linux（macOS 理论上支持但需要 root 权限）
- Root/sudo 权限（原始套接字需要）

```bash
go build -o shadow-gre ./cmd/shadow-gre
```

## 使用方法

### 服务端模式

在服务端运行，接收 GRE 流量并转发到后端服务：

```bash
sudo ./shadow-gre \
  -mode server \
  -local 0.0.0.0 \
  -backend 127.0.0.1:8388 \
  -password YOUR_PASSWORD
```

### 客户端模式

在客户端运行，监听 TCP 连接并通过 GRE 转发到服务端：

```bash
sudo ./shadow-gre \
  -mode client \
  -listen 0.0.0.0:1080 \
  -local 0.0.0.0 \
  -remote SERVER_IP \
  -password YOUR_PASSWORD
```

### 参数说明

| 参数 | 描述 |
|-----------|-------------|
| `-mode` | 运行模式：`client` 或 `server` |
| `-listen` | TCP 监听地址（仅客户端模式） |
| `-local` | GRE 套接字绑定的本地 IP 地址 |
| `-remote` | 服务端 IP 地址（仅客户端模式） |
| `-backend` | 后端服务地址（仅服务端模式） |
| `-password` | 用于生成 GRE Key 的共享密码 |

## Shadowsocks 使用示例

### 服务端配置

1. 在 `127.0.0.1:8388` 运行 Shadowsocks 服务器
2. 启动 shadow-gre 服务端：

```bash
sudo ./shadow-gre -mode server -local 0.0.0.0 -backend 127.0.0.1:8388 -password YOUR_PASSWORD
```

### 客户端配置

1. 启动 shadow-gre 客户端：

```bash
sudo ./shadow-gre -mode client -listen 0.0.0.0:1080 -local 0.0.0.0 -remote SERVER_IP -password YOUR_PASSWORD
```

2. 配置 Shadowsocks 客户端连接到 `127.0.0.1:1080`

## 注意事项

1. **需要 Root 权限**：原始套接字操作需要 root/sudo 权限
2. **防火墙**：确保防火墙允许 GRE 协议（IP 协议 47）
3. **NAT 问题**：GRE 是 IP 层协议，某些 NAT 设备可能不支持

## 协议规范

### GRE 头部格式

使用标准 GRE 格式（RFC 2784 + RFC 2890）：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|C| |K|S|       Reserved0       |      Protocol Type (0x6558)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Key (来自密码)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload...                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 可靠传输协议

基于 GRE 的自定义可靠协议，保证数据传输：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Stream ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Flags     |                Sequence Number                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Seq (cont)   |              ACK Number (可选)                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| ACK (cont)    |  SACK Count   |         SACK Blocks...        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Payload...                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**标志位：**
- `0x01` DATA - 包含有效载荷数据
- `0x02` ACK - 包含确认信息
- `0x04` CLOSE - 流关闭
- `0x08` SYN - 流同步
- `0x10` SACK - 包含选择性确认块

**可靠性特性：**
- 累积 ACK 和 SACK（选择性确认）
- 自适应 RTO 计算（RFC 6298）
- 3 个重复 ACK 触发快速重传
- 滑动窗口流控（128 个数据包）
- 乱序数据包缓冲
- 序列号回绕处理

## 许可证

MIT
