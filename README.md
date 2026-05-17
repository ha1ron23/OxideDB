# OxideDB

A high-performance, asynchronous, multi-threaded In-Memory Key-Value database written in Rust. It utilizes a structural `BTreeMap` architecture for data efficiency, paired with a custom RESP streaming network buffer and ACID-compliant Write-Ahead Logging (WAL) for bulletproof resilience.

## Features
- **Async Core:** High-concurrency network polling powered by `Tokio`.
- **Memory Optimized:** Leverages Rust's native `BTreeMap` over standard HashMap to prevent fragmentation.
- **Redis Compatible:** Implements a strict streaming RESP (Redis Serialization Protocol) parser.
- **Persistence Layer:** Built-in Write-Ahead Logging (`appendonly.wal`) for complete data recovery on boot.
- **Production Logs:** Context-rich structural tracing logging instead of raw stdout writes.

## Getting Started

### Prerequisites
Make sure you have the Rust toolchain installed. If not, install it using:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://rustup.rs | sh
```

### Installation & Build
Clone this repository to your local machine and navigate into the project directory:
```bash
git clone https://github.com
cd OxideDB
```

Build the project into a release binary to achieve maximum optimizations:
```bash
cargo build --release
```

### Running the Server
You can launch the core engine directly via Cargo:
```bash
cargo run
```
The server will boot up and spin an active socket at `127.0.0.1:6379`.

## Testing the Database

### Method 1: Using `redis-cli` (Recommended)
Since OxideDB is 100% compliant with the Redis Protocol, you can attach any standard client to it:
```bash
redis-cli -p 6379
```
Inside the prompt, interact natively:
```text
127.0.0.1:6379> SET user "ha1ron23"
OK
127.0.0.1:6379> GET user
"ha1ron23"
```

### Method 2: Raw TCP Stream via `nc`
For debugging network pipes, stream raw RESP frames into the open socket:
```bash
echo -e "*3\n\$3\nSET\n\$4\nname\n\$4\nrust" | nc 127.0.0.1 6379
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.
