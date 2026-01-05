# shadow-gre

TCP over GRE 隧道 - 将 TCP 流量封装成 GRE 协议（IP Protocol 47）进行传输。

## 架构

```
┌─────────────────┐                                    ┌─────────────────┐
│   Application   │                                    │  Backend Server │
│  (e.g. browser) │                                    │ (e.g. ss-server)│
└────────┬────────┘                                    └────────▲────────┘
         │ TCP                                                  │ TCP
         ▼                                                      │
┌─────────────────┐         GRE Protocol 47           ┌─────────┴────────┐
│  shadow-gre     │ ◄─────────────────────────────────► │  shadow-gre     │
│  (client mode)  │         Raw IP Socket              │  (server mode)  │
└─────────────────┘                                    └──────────────────┘
```

## 特性

- 使用真正的 GRE 协议（IP Protocol 47）
- 支持多连接复用
- 简单的密码认证（通过 GRE Key 字段）
- Docker 部署支持

## 要求

- Go 1.21+
- Linux 系统（macOS 理论上支持但需要 root）
- Root/sudo 权限（raw socket 需要）
- 网络环境允许 GRE 协议通过

## 编译

```bash
go build -o shadow-gre ./cmd/shadow-gre
```

## 使用方法

### Server 模式

在服务端运行，接收 GRE 流量并转发到后端服务：

```bash
sudo ./shadow-gre \
  -mode server \
  -local 0.0.0.0 \
  -backend 127.0.0.1:8388 \
  -password YOUR_PASSWORD
```

### Client 模式

在客户端运行，监听 TCP 连接并通过 GRE 转发到服务器：

```bash
sudo ./shadow-gre \
  -mode client \
  -listen 0.0.0.0:1080 \
  -local 0.0.0.0 \
  -remote SERVER_IP \
  -password YOUR_PASSWORD
```

### 参数说明

| 参数 | 说明 |
|------|------|
| `-mode` | 运行模式：`client` 或 `server` |
| `-listen` | TCP 监听地址（仅 client 模式） |
| `-local` | 本地 IP 地址，用于绑定 GRE socket |
| `-remote` | 服务器 IP 地址（仅 client 模式） |
| `-backend` | 后端服务地址（仅 server 模式） |
| `-password` | 共享密码，用于生成 GRE Key |

## Docker 部署

### Server 端

```bash
# 编辑密码
export PASSWORD=your_secure_password

# 启动
docker-compose -f docker-compose.server.yml up -d
```

### Client 端

```bash
# 设置服务器 IP 和密码
export SERVER_IP=your_server_ip
export PASSWORD=your_secure_password

# 启动
docker-compose -f docker-compose.client.yml up -d
```

## 配合 Shadowsocks 使用示例

### 服务端配置

1. 创建 Shadowsocks 配置文件 `config.json`:

```json
{
    "server": "127.0.0.1",
    "server_port": 8388,
    "password": "ss_password",
    "method": "chacha20-ietf-poly1305"
}
```

2. 使用 docker-compose.server.yml 启动

### 客户端配置

1. 启动 shadow-gre client，监听 1080 端口
2. 配置 Shadowsocks 客户端连接 127.0.0.1:1080

## 注意事项

1. **需要 Root 权限**: Raw socket 操作需要 root/sudo 权限
2. **防火墙**: 确保防火墙允许 GRE 协议（IP Protocol 47）通过
3. **NAT 问题**: GRE 是 IP 层协议，某些 NAT 设备可能不支持
4. **Docker 权限**: 需要 `NET_RAW` 和 `NET_ADMIN` capabilities

## 协议说明

### GRE 头部格式

使用标准 GRE 格式（RFC 2784 + RFC 2890）：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|C| |K|S|       Reserved0       |      Protocol Type (0x6558)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Key (from password)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Sequence Number                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload...                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 隧道帧格式

在 GRE 载荷中使用自定义帧格式进行连接复用：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Type      |                 Connection ID                 |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Conn ID (cont)|                 Sequence Number               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Seq (cont)    |        Length         |       Payload...      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

帧类型：
- `0x00`: DATA - 数据帧
- `0x01`: SYN - 连接建立
- `0x02`: SYN-ACK - 连接确认
- `0x03`: FIN - 连接关闭
- `0x04`: FIN-ACK - 关闭确认
- `0x05`: PING - 心跳
- `0x06`: PONG - 心跳响应

## 许可证

MIT
