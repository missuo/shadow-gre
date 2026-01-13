# Shadow GRE Project Guidelines

## Project Overview

Shadow GRE is a high-performance tunneling system that implements a reliable transport layer over UDP with advanced features like selective acknowledgments (SACK), congestion control, and efficient retransmission mechanisms.

## Architecture

- **Client-Server Model**: Implements a client that connects to a remote server through the tunnel
- **Reliable Layer**: Custom reliable transport protocol built on UDP with SACK-based retransmission
- **Performance Focus**: Optimized for high throughput with careful attention to deadlocks and concurrency

## Development Guidelines

### Code Organization

- **tunnel/**: Core tunneling protocol implementation
- **client/**: Client-side connection logic
- **server/**: Server-side handling
- Keep performance-critical paths optimized
- Pay attention to concurrency and potential deadlocks

### Commit Message Convention

Follow the conventional commit format as specified in the global CLAUDE.md:

```
<type>(<scope>): <short summary>
```

**Common scopes for this project:**
- `tunnel`: Tunneling protocol and reliable layer
- `client`: Client implementation
- `server`: Server implementation
- `project`: Project-wide changes
- `docs`: Documentation updates

**Recent examples from this project:**
```
chore(client): remove unused constants
docs(project): add design document
fix(tunnel): improve SACK-based retransmission logic
perf(tunnel): optimize reliable layer for higher throughput
```

### Performance Considerations

- This is a performance-sensitive project
- Profile before optimizing
- Be mindful of allocations in hot paths
- Test thoroughly for race conditions and deadlocks

### Testing

- Test concurrent scenarios thoroughly
- Verify retransmission logic with packet loss
- Benchmark performance changes

## Key Areas of Focus

1. **Reliability**: Ensure packet delivery guarantees
2. **Performance**: Maintain high throughput
3. **Concurrency Safety**: Avoid deadlocks and race conditions
4. **Network Efficiency**: Optimize SACK and retransmission logic
