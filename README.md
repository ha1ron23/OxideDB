# OxideDB

A high-performance, asynchronous, multi-threaded In-Memory Key-Value database written in Rust using Tokio.

## 🛠️ Project Roadmap
- [x] Asynchronous TCP network engine powered by `Tokio`.
- [x] Thread-safe In-Memory storage architecture (`Arc<Mutex<HashMap>>`).
- [x] Redis-compatible RESP protocol parser.
- [x] Write-Ahead Logging (WAL) engine for disk persistence.
- [x] Memory optimization using B-Trees instead of standard HashMap.

## 🚀 Getting Started

### Prerequisites
Make sure you have Rust and Cargo installed on your system.

### Running the Server
```bash
cargo run
```

### Testing the Database
You can easily connect to the database server using `netcat` or `telnet`:
```bash
nc 127.0.0.1 6379
```

Inside the interactive session, you can execute commands:
```text
SET user_name ha1ron23
OK

GET user_name
VALUE: ha1ron23
```

## 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
