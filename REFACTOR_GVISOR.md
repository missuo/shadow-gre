# Shadow-GRE gVisor TCP Stack 重构方案

## 1. 重构目标

使用 gVisor 的 netstack 替换当前自定义的可靠传输层实现，以获得：
- 更成熟的 TCP 实现（完整的 SACK、拥塞控制、流量控制）
- 更好的性能（优化的内存管理、批量处理）
- 更低的维护成本（删除 ~1000 行自定义协议代码）
- 更强的可配置性（多种拥塞控制算法可选）

## 2. 架构设计

### 2.1 当前架构
```
TCP Application
    ↓
TCP Listener (port 1080)
    ↓
Custom Reliable Layer (reliable.go)
    ├── Sequence/Ack Management
    ├── SACK Implementation
    ├── RTT Estimation (RFC 6298)
    ├── Fast Retransmit
    ├── Simplified BBR
    └── Window Management
    ↓
GRE Encapsulation
    ↓
Raw Socket (IP Protocol 47)
```

### 2.2 重构后架构
```
TCP Application
    ↓
TCP Listener (port 1080)
    ↓
gVisor TCP Stack (netstack)
    ├── Complete TCP Implementation
    ├── Built-in SACK Support
    ├── Multiple Congestion Control (Cubic/BBR/Reno)
    ├── Flow Control
    └── Advanced Retransmission
    ↓
Custom Link Layer Adapter (实现 stack.LinkEndpoint)
    ↓
GRE Encapsulation
    ↓
Raw Socket (IP Protocol 47)
```

### 2.3 核心组件

#### A. Link Layer Adapter (新增)
**文件**: `pkg/tunnel/gre_link.go`

实现 gVisor 的 `stack.LinkEndpoint` 接口，连接 netstack 和 GRE 传输层：

```go
type GRELinkEndpoint struct {
    dispatcher     stack.NetworkDispatcher
    mtu            uint32
    linkAddr       tcpip.LinkAddress
    transport      *transport.RawTransport  // 或 ServerTransport
    capabilities   stack.LinkEndpointCapabilities

    // 发送队列
    sendQueue      chan *stack.PacketBuffer

    // 统计信息
    stats          Stats
}

// 实现 stack.LinkEndpoint 接口
func (e *GRELinkEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error)
func (e *GRELinkEndpoint) Attach(dispatcher stack.NetworkDispatcher)
func (e *GRELinkEndpoint) MTU() uint32
func (e *GRELinkEndpoint) Capabilities() stack.LinkEndpointCapabilities
func (e *GRELinkEndpoint) MaxHeaderLength() uint16
// ... 其他接口方法
```

**关键逻辑**：
1. **发送路径**: netstack → WritePackets() → GRE 封装 → Raw Socket
2. **接收路径**: Raw Socket → GRE 解封装 → dispatcher.DeliverNetworkPacket() → netstack

#### B. TCP Stack Manager (新增)
**文件**: `pkg/tunnel/tcp_stack.go`

管理 gVisor netstack 实例：

```go
type TCPStackManager struct {
    stack          *stack.Stack
    linkEP         *GRELinkEndpoint
    nicID          tcpip.NICID

    // 每个 StreamID 对应一个虚拟 TCP 连接
    streams        map[uint32]*TCPStream
    streamsMu      sync.RWMutex

    // IP 地址分配（为每个 stream 分配虚拟 IP）
    ipAllocator    *IPAllocator
}

type TCPStream struct {
    streamID       uint32
    endpoint       tcpip.Endpoint  // gVisor TCP endpoint
    localAddr      tcpip.FullAddress
    remoteAddr     tcpip.FullAddress

    // 与应用层的连接
    appConn        net.Conn

    // 数据通道
    readCh         chan []byte
    writeCh        chan []byte
}
```

#### C. IP 地址分配器 (新增)
**文件**: `pkg/tunnel/ip_allocator.go`

为每个 TCP stream 分配虚拟 IP 地址：

```go
type IPAllocator struct {
    // 使用私有地址段，例如 10.255.0.0/16
    baseIP         net.IP
    nextIP         uint32
    usedIPs        map[uint32]tcpip.Address
    mu             sync.Mutex
}

// 为 streamID 分配 IP
func (a *IPAllocator) AllocateIP(streamID uint32) tcpip.Address
func (a *IPAllocator) ReleaseIP(streamID uint32)
```

## 3. 实现细节

### 3.1 Client 端实现

**修改文件**: `pkg/shadowgre/client.go`

```go
type Client struct {
    // 原有字段
    listenAddr     string
    remoteIP       net.IP
    key            uint32

    // 替换 ReliableManager 为 TCPStackManager
    tcpStackMgr    *tunnel.TCPStackManager

    // 其他保持不变
    transport      *transport.RawTransport
    bufPool        *sync.Pool
}

func (c *Client) handleConnection(conn net.Conn) {
    streamID := c.allocateStreamID()

    // 创建 gVisor TCP stream
    stream, err := c.tcpStackMgr.CreateStream(streamID, conn)
    if err != nil {
        log.Printf("Failed to create stream: %v", err)
        conn.Close()
        return
    }

    // gVisor 会自动处理可靠传输，只需要桥接数据
    go c.bridgeConnection(conn, stream)
}

func (c *Client) bridgeConnection(appConn net.Conn, stream *tunnel.TCPStream) {
    defer appConn.Close()
    defer c.tcpStackMgr.CloseStream(stream.streamID)

    // 双向数据转发
    go io.Copy(stream.endpoint, appConn)  // App → gVisor
    io.Copy(appConn, stream.endpoint)     // gVisor → App
}
```

### 3.2 Server 端实现

**修改文件**: `pkg/shadowgre/server.go`

```go
type Server struct {
    listenIP       net.IP
    backendAddr    string
    key            uint32

    // 每个客户端 IP 一个 TCPStackManager
    clients        map[string]*ClientState
    clientsMu      sync.RWMutex

    transport      *transport.ServerTransport
}

type ClientState struct {
    clientIP       net.IP
    tcpStackMgr    *tunnel.TCPStackManager
    streams        map[uint32]*ServerStream
    streamsMu      sync.RWMutex
}

func (s *Server) handleGREPacket(clientIP net.IP, payload []byte) {
    cs := s.getOrCreateClient(clientIP)

    // 将 GRE payload 注入到 gVisor netstack
    cs.tcpStackMgr.InjectPacket(payload)
}
```

### 3.3 GRE 封装格式

保持当前的 GRE 格式不变：

```
+------------------+
| IP Header        |
+------------------+
| GRE Header       |
|  - Flags: 0x2000 |
|  - Proto: 0x0800 | ← IPv4
|  - Key: xxxxxx   |
+------------------+
| IP Packet (内层)  | ← gVisor 生成的 IP 包
|  - Src: 10.255.x.x (虚拟 IP)
|  - Dst: 10.255.y.y (虚拟 IP)
|  - Proto: TCP    |
|  +---------------+
|  | TCP Segment   | ← gVisor TCP stack 处理
|  |  - SACK       |
|  |  - Timestamps |
|  |  - Window     |
|  +---------------+
+------------------+
```

**关键变化**：
- 不再需要自定义的 Reliable Header（StreamID + Flags + Seq + Ack + SACK）
- 使用标准的内层 IP + TCP 封装
- StreamID 映射到虚拟 IP 地址

### 3.4 StreamID 到虚拟 IP 的映射

```go
// Client: 10.255.0.x/16
func streamIDToClientIP(streamID uint32) tcpip.Address {
    return tcpip.Address(net.IPv4(10, 255, byte(streamID>>8), byte(streamID)))
}

// Server: 10.254.0.x/16
func streamIDToServerIP(streamID uint32) tcpip.Address {
    return tcpip.Address(net.IPv4(10, 254, byte(streamID>>8), byte(streamID)))
}

// 从 IP 反解 StreamID
func ipToStreamID(ip tcpip.Address) uint32 {
    ipBytes := []byte(ip)
    return uint32(ipBytes[2])<<8 | uint32(ipBytes[3])
}
```

## 4. 配置和优化

### 4.1 gVisor TCP Stack 配置

```go
func NewTCPStack() (*stack.Stack, error) {
    s := stack.New(stack.Options{
        NetworkProtocols: []stack.NetworkProtocolFactory{
            ipv4.NewProtocol,
        },
        TransportProtocols: []stack.TransportProtocolFactory{
            tcp.NewProtocol,
        },
    })

    // 启用 SACK
    {
        opt := tcpip.TCPSACKEnabled(true)
        s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
    }

    // 设置拥塞控制算法为 BBR
    {
        opt := tcpip.CongestionControlOption("bbr")
        s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
    }

    // 优化发送/接收缓冲区
    {
        opt := tcpip.TCPSendBufferSizeRangeOption{
            Min:     4096,
            Default: 524288,  // 512KB
            Max:     4194304, // 4MB
        }
        s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
    }

    {
        opt := tcpip.TCPReceiveBufferSizeRangeOption{
            Min:     4096,
            Default: 524288,
            Max:     4194304,
        }
        s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
    }

    // 启用 TCP Fast Open (可选)
    // 调整重传参数等...

    return s, nil
}
```

### 4.2 性能调优参数

| 参数 | 建议值 | 说明 |
|------|--------|------|
| MTU | 1400 | 考虑 GRE 和内层 IP/TCP 头部开销 |
| TCP Send Buffer | 512KB - 4MB | 根据带宽延迟积调整 |
| TCP Recv Buffer | 512KB - 4MB | 同上 |
| SACK | Enabled | 必须启用 |
| Congestion Control | BBR | 推荐 BBR，也可选 Cubic |
| Timestamps | Enabled | 提高 RTT 测量精度 |
| Window Scaling | Enabled | 支持大窗口 |

## 5. 实现步骤

### Phase 1: 基础框架 (1-2 天)
1. ✅ 添加 gVisor 依赖：`go get gvisor.dev/gvisor`
2. ✅ 实现 GRELinkEndpoint (pkg/tunnel/gre_link.go)
3. ✅ 实现 IPAllocator (pkg/tunnel/ip_allocator.go)
4. ✅ 实现 TCPStackManager 基础结构 (pkg/tunnel/tcp_stack.go)

### Phase 2: Client 端重构 (2-3 天)
5. ✅ 修改 Client 使用 TCPStackManager
6. ✅ 实现 TCP stream 创建和销毁
7. ✅ 实现数据桥接逻辑
8. ✅ 测试基本连通性

### Phase 3: Server 端重构 (2-3 天)
9. ✅ 修改 Server 使用 TCPStackManager
10. ✅ 实现多客户端管理
11. ✅ 测试 Client-Server 通信

### Phase 4: 优化和测试 (3-5 天)
12. ✅ 性能测试（吞吐量、延迟）
13. ✅ 丢包场景测试
14. ✅ 并发连接测试
15. ✅ 内存和 CPU 优化
16. ✅ 与旧实现对比

### Phase 5: 清理和文档 (1-2 天)
17. ✅ 删除旧的 reliable.go 代码
18. ✅ 更新 DESIGN.md
19. ✅ 更新 README
20. ✅ 添加配置文档

## 6. 兼容性考虑

### 6.1 协议兼容性
- ⚠️ **不兼容**: 新旧版本无法互通（GRE payload 格式完全不同）
- 需要同时升级 Client 和 Server
- 建议使用版本号或 GRE Key 区分新旧协议

### 6.2 迁移策略
1. **双版本共存**:
   - Server 同时监听两个 GRE Key
   - 旧客户端使用 key1（自定义协议）
   - 新客户端使用 key2（gVisor TCP）

2. **逐步迁移**:
   - 先升级 Server 支持双协议
   - 逐步升级 Client
   - 废弃旧协议支持

## 7. 预期收益

### 7.1 代码简化
- 删除 reliable.go (~1000 行)
- 新增代码约 500-700 行
- 净减少约 300-500 行代码
- 维护复杂度大幅降低

### 7.2 性能提升
- ✅ 更优的拥塞控制（BBR vs 简化 BBR）
- ✅ 更精确的 RTT 测量和重传
- ✅ 更好的零拷贝支持
- ✅ 批量数据包处理
- 预期吞吐量提升 **20-50%**

### 7.3 功能增强
- ✅ 完整的 TCP 特性支持
- ✅ 可配置的拥塞控制算法
- ✅ 更好的流量控制
- ✅ TCP Fast Open 支持（可选）

## 8. 风险和挑战

### 8.1 技术风险
1. **学习曲线**: gVisor netstack API 较复杂
2. **调试难度**: 用户态网络栈的调试比较困难
3. **内存占用**: gVisor stack 可能比自定义实现占用更多内存

### 8.2 缓解措施
1. 先在实验分支开发，充分测试后再合并
2. 保留旧实现作为 fallback（通过编译标签）
3. 添加详细的日志和监控

## 9. 参考资源

- [gVisor Netstack 文档](https://gvisor.dev/docs/user_guide/networking/)
- [gVisor TCP 实现](https://github.com/google/gvisor/tree/master/pkg/tcpip/transport/tcp)
- [BBR 算法论文](https://queue.acm.org/detail.cfm?id=3022184)
- [TCP SACK RFC 2018](https://datatracker.ietf.org/doc/html/rfc2018)

## 10. 总结

使用 gVisor TCP stack 重构是一个**高收益、中等风险**的技术决策：

✅ **优势明显**:
- 代码简化
- 性能提升
- 功能增强
- 维护性提升

⚠️ **需要注意**:
- 协议不兼容（需要同时升级）
- 需要一定的开发和测试时间
- 内存占用可能增加

**建议**: 值得投入精力重构！特别是对于追求高性能和稳定性的生产环境。
