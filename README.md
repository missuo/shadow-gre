# Shadow GRE

[![Release](https://img.shields.io/github/v/release/missuo/shadow-gre)](https://github.com/missuo/shadow-gre/releases)
[![License](https://img.shields.io/github/license/missuo/shadow-gre)](LICENSE)

English | [中文](README_CN.md)

TCP over GRE tunnel - Encapsulate TCP traffic into GRE protocol (IP Protocol 47) for transmission with reliable delivery.

## Architecture

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

## Features

- Uses real GRE protocol (IP Protocol 47)
- **Reliable transport layer** with retransmission and SACK support
- Adaptive RTO based on RTT measurement (RFC 6298)
- Supports multiple connection multiplexing
- Simple authentication via GRE Key field

## Installation

### Download Pre-built Binaries

Download the latest release from [GitHub Releases](https://github.com/missuo/shadow-gre/releases).

Available binaries:
- `shadow-gre-linux-amd64` - Linux x86_64
- `shadow-gre-linux-arm64` - Linux ARM64
- `shadow-gre-linux-armv7` - Linux ARMv7
- `shadow-gre-darwin-amd64` - macOS Intel
- `shadow-gre-darwin-arm64` - macOS Apple Silicon
- `shadow-gre-freebsd-amd64` - FreeBSD x86_64

### Build from Source

Requirements:
- Go 1.21+
- Linux (macOS theoretically supported but requires root)
- Root/sudo privileges (required for raw sockets)

```bash
go build -o shadow-gre ./cmd/shadow-gre
```

## Usage

### Server Mode

Run on the server side to receive GRE traffic and forward to backend services:

```bash
sudo ./shadow-gre \
  -mode server \
  -local 0.0.0.0 \
  -backend 127.0.0.1:8388 \
  -password YOUR_PASSWORD
```

### Client Mode

Run on the client side to listen for TCP connections and forward via GRE to the server:

```bash
sudo ./shadow-gre \
  -mode client \
  -listen 0.0.0.0:1080 \
  -local 0.0.0.0 \
  -remote SERVER_IP \
  -password YOUR_PASSWORD
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `-mode` | Running mode: `client` or `server` |
| `-listen` | TCP listen address (client mode only) |
| `-local` | Local IP address for GRE socket binding |
| `-remote` | Server IP address (client mode only) |
| `-backend` | Backend service address (server mode only) |
| `-password` | Shared password for generating GRE Key |

## Example with Shadowsocks

### Server Configuration

1. Run Shadowsocks server on `127.0.0.1:8388`
2. Start shadow-gre server:

```bash
sudo ./shadow-gre -mode server -local 0.0.0.0 -backend 127.0.0.1:8388 -password YOUR_PASSWORD
```

### Client Configuration

1. Start shadow-gre client:

```bash
sudo ./shadow-gre -mode client -listen 0.0.0.0:1080 -local 0.0.0.0 -remote SERVER_IP -password YOUR_PASSWORD
```

2. Configure Shadowsocks client to connect to `127.0.0.1:1080`

## Important Notes

1. **Root Privileges Required**: Raw socket operations require root/sudo privileges
2. **Firewall**: Ensure firewall allows GRE protocol (IP Protocol 47)
3. **NAT Issues**: GRE is an IP layer protocol, some NAT devices may not support it

## Protocol Specification

### GRE Header Format

Uses standard GRE format (RFC 2784 + RFC 2890):

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|C| |K|S|       Reserved0       |      Protocol Type (0x6558)   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Key (from password)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload...                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Reliable Transport Protocol

Custom reliable protocol over GRE for guaranteed delivery:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Stream ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Flags     |                Sequence Number                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Seq (cont)   |              ACK Number (optional)            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| ACK (cont)    |  SACK Count   |         SACK Blocks...        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Payload...                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**Flags:**
- `0x01` DATA - Contains payload data
- `0x02` ACK - Contains acknowledgment
- `0x04` CLOSE - Stream close
- `0x08` SYN - Stream synchronization
- `0x10` SACK - Contains selective ACK blocks

**Reliability Features:**
- Cumulative ACK with SACK (Selective Acknowledgment)
- Adaptive RTO calculation (RFC 6298)
- Fast retransmit on 3 duplicate ACKs
- Sliding window flow control (128 packets)
- Out-of-order packet buffering
- Sequence number wraparound handling

## License

MIT
