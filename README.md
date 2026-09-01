<div align="center">
  <h1>Java Redis Server</h1>
  <p><strong>Custom Concurrent In-Memory Redis Server Built from Scratch in Java</strong></p>
  <p>
    <a href="https://github.com/MahinShahriar/custom-redis-java"><img src="https://img.shields.io/badge/Java-17%2B-ED8B00?logo=openjdk&logoColor=white" alt="Java 17+"></a>
    <a href="https://github.com/MahinShahriar/custom-redis-java"><img src="https://img.shields.io/badge/Protocol-RESP-DC382D?logo=redis&logoColor=white" alt="RESP Protocol"></a>
  </p>
</div>

---

## Overview

**Java Redis Server** is a multi-threaded, in-memory data store built from the ground up using core Java concurrency primitives and TCP networking. It supports core Redis data structures (Strings, Lists, and Hashes), hybrid TTL key eviction, raw RESP wire protocol parsing, and append-only file (AOF) disk persistence for complete state recovery across server restarts.

**Current Version:** 1.0

---

## Features

- ⚡ **Multi-Threaded TCP Server** - Concurrent client execution powered by Java `ServerSocket` and asynchronous worker threads
- 🔑 **String Key-Value Engine** - Sub-millisecond `SET`, `GET`, `DEL`, and `EXISTS` operations
- 🔢 **Atomic Operations** - Safe atomic incrementing and decrementing via `INCR` and `DECR`
- 📑 **List Data Structures** - Full push and pop queue/stack mechanics via `RPUSH`, `LPUSH`, `LPOP`, and `RPOP`
- 🗂️ **Hash Map Support** - Multi-field object storage support via `HSET`, `HGET`, and `HGETALL`
- ⏳ **Hybrid TTL Eviction** - Active 1-second background thread sweeper combined with passive access checks
- 🌐 **RESP Protocol Compatibility** - Custom array parser compatible with standard `redis-cli` and official clients
- 💾 **AOF Disk Persistence** - Append-Only File (`database.aof`) logging for complete data recovery on restart

---

## Installation

1. **Clone** this repository:
   ```sh
   git clone [https://github.com/MahinShahriar/custom-redis-java.git](https://github.com/MahinShahriar/custom-redis-java.git)

```

2. **Navigate** into the project directory:
```sh
cd custom-redis-java

```


3. **Compile** the Java source code:
```sh
javac Server.java

```


4. **Run** the server on port 6379:
```sh
java Server

```



---

## Usage

### Connecting via PowerShell / TCP Direct

```powershell
$client = New-Object System.Net.Sockets.TcpClient("127.0.0.1", 6379)
$stream =$client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream); $writer.AutoFlush =$true
$reader = New-Object System.IO.StreamReader($stream)

# Set a string with 5-second TTL
$writer.WriteLine("SET myKey Hello EX 5")
$reader.ReadLine()

# Get field from a Hash
$writer.WriteLine("HSET user:100 name Mahin email dev@test.com")
$reader.ReadLine()

$writer.WriteLine("HGET user:100 name")
$reader.ReadLine()

```

### Connecting via Official `redis-cli`

```bash
redis-cli -p 6379 SET server:status active
redis-cli -p 6379 GET server:status
redis-cli -p 6379 HGETALL user:100

```

---

## Command Reference

### Strings & Key Management

* `SET <key> <value> [EX seconds]` - Set key-value pair with optional TTL expiration.
* `GET <key>` - Retrieve value for key (returns `(nil)` if expired or non-existent).
* `EXISTS <key1> [key2...]` - Count how many specified keys exist.
* `DEL <key1> [key2...]` - Delete one or more keys.
* `INCR <key>` / `DECR <key>` - Atomically increment or decrement an integer key.

### Lists

* `RPUSH <key> <value1> [value2...]` - Append values to the tail of a list.
* `LPUSH <key> <value1> [value2...]` - Prepend values to the head of a list.
* `RPOP <key>` / `LPOP <key>` - Remove and return element from list tail or head.

### Hashes

* `HSET <key> <field1> <value1> [field2 value2...]` - Set specified fields in a hash.
* `HGET <key> <field>` - Retrieve the value associated with field in hash.
* `HGETALL <key>` - Get all fields and values stored in a hash.

---

## Requirements

* Java JDK 17 or higher
* Terminal, PowerShell, or standard `redis-cli`

---

## Architecture Blueprint

| Layer | Responsibility | Mechanism |
| --- | --- | --- |
| **Networking** | Concurrent Client Connections | `ServerSocket` + Thread-per-Client Pattern |
| **In-Memory Store** | Thread-Safe Memory Operations | `ConcurrentHashMap` & nested collections |
| **Eviction** | Memory Cleanup & TTL | Passive check on lookup + `ScheduledExecutorService` active sweeper |
| **Persistence** | Data Restoration | Sequential write-ahead logging to `database.aof` |

---

## Author

Developed by [Mahin Shahriar](https://www.google.com/search?q=https://github.com/MahinShahriar)

---

## License

This project is open-source and available under the MIT License.

```

```