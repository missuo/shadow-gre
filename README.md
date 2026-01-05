# Shadow GRE

TCP over GRE tunnel - Encapsulate TCP traffic into GRE protocol (IP Protocol 47) for transmission.

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
- Supports multiple connection multiplexing
- Simple authentication via GRE Key field
- Docker deployment support

## Requirements

- Go 1.21+
- Linux (macOS theoretically supported but requires root)
- Root/sudo privileges (required for raw sockets)
- Network environment that allows GRE protocol

## Build

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

## Docker Deployment

### Server Side

```bash
# Set password
export PASSWORD=your_secure_password

# Start
docker-compose -f docker-compose.server.yml up -d
```

### Client Side

```bash
# Set server IP and password
export SERVER_IP=your_server_ip
export PASSWORD=your_secure_password

# Start
docker-compose -f docker-compose.client.yml up -d
```

## Example with Shadowsocks

### Server Configuration

1. Create Shadowsocks config file `config.json`:

```json
{
    "server": "127.0.0.1",
    "server_port": 8388,
    "password": "ss_password",
    "method": "chacha20-ietf-poly1305"
}
```

2. Start using docker-compose.server.yml

### Client Configuration

1. Start shadow-gre client listening on port 1080
2. Configure Shadowsocks client to connect to 127.0.0.1:1080

## Important Notes

1. **Root Privileges Required**: Raw socket operations require root/sudo privileges
2. **Firewall**: Ensure firewall allows GRE protocol (IP Protocol 47)
3. **NAT Issues**: GRE is an IP layer protocol, some NAT devices may not support it
4. **Docker Permissions**: Requires `NET_RAW` and `NET_ADMIN` capabilities

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
|                         Sequence Number                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload...                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Tunnel Frame Format

Custom frame format in GRE payload for connection multiplexing:

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

Frame Types:
- `0x00`: DATA - Data frame
- `0x01`: SYN - Connection establishment
- `0x02`: SYN-ACK - Connection acknowledgment
- `0x03`: FIN - Connection close
- `0x04`: FIN-ACK - Close acknowledgment
- `0x05`: PING - Heartbeat
- `0x06`: PONG - Heartbeat response

## License

MIT
