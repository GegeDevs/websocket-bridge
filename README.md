# websocket-bridge

Rust implementation of the SSH WebSocket bridge used by AutoScript.

It accepts a WebSocket-style upgrade request, reads `X-Real-Host`, connects to that TCP endpoint, returns the same `101 GegeVPS` response as the Python implementation, and proxies bytes in both directions.

## Build

```sh
cargo build --release
```

## Run

```sh
target/release/websocket-bridge 8880
target/release/websocket-bridge --bind 127.0.0.1 --port 8880
```

The positional port and `--bind/--port` options match the existing Python entrypoint.

## Test

```sh
cargo fmt --check
cargo test
```

## Behavior notes

- `X-Real-Host: host:port` selects the target.
- Missing `X-Real-Host` falls back to `127.0.0.1:<listen-port>`.
- `X-Split` consumes one additional read before proxying.
- Connections use a 10-second connect timeout and 60-second idle timeout.
- The original Python scripts are not deleted or changed by this project.
