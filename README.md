# Mini-Kafka

A fully functional distributed log broker built from scratch in Go, with a real-time Next.js monitoring dashboard. Implements core concepts from Apache Kafka at a minimal scale for educational and demonstration purposes.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Go Broker                         │
│                                                     │
│  TCP :9092 ──┐                                      │
│              ├──▶ Topic Manager ──▶ Partitions       │
│  HTTP :8080 ─┘       │              │               │
│                      │         CommitLog             │
│              Consumer Groups    │    │               │
│              (offset tracking)  Segments  Indexes    │
│                                (mmap)   (mmap)      │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│              Next.js Dashboard :3000                 │
│                                                     │
│  Stats ─── Topics ─── Messages ─── Consumer Groups  │
│           (polls HTTP API every 2s)                  │
└─────────────────────────────────────────────────────┘
```

## Features

### Storage Engine
- **Append-only immutable log** — messages are strictly appended, never modified
- **Memory-mapped files (mmap)** — index files are memory-mapped via `syscall.Mmap` for O(1) offset lookups
- **Zero-copy transfer** — uses `io.Copy` which triggers `sendfile(2)` on Linux to transfer data directly from disk to socket
- **Log segments** — automatic rotation at 10MB boundaries with offset-indexed `.log` + `.index` file pairs
- **Cross-platform** — real mmap/sendfile on Linux/macOS, buffered I/O fallback on Windows (via build tags)

### Broker
- **Topics & Partitions** — structured directory layout: `data/<topic>/partition-<N>/`
- **Binary TCP protocol** — custom wire protocol with produce, fetch, create-topic, list-topics, commit-offset, fetch-offset operations
- **Consumer groups** — offset tracking with JSON persistence
- **HTTP REST API** — full CRUD for topics, produce/consume, stats, consumer groups (used by the dashboard)

### Dashboard
- **Real-time monitoring** — polls broker every 2 seconds
- **Stat cards** — total topics, partitions, messages, throughput
- **Topic management** — view all topics, create new ones inline
- **Live message stream** — auto-scrolling feed of recent messages
- **Consumer group tracking** — committed offsets, latest offsets, lag per partition

## Quick Start

### Docker (Recommended)

```bash
# Build and run everything
docker-compose up --build

# Or in detached mode
docker-compose up --build -d
```

Then open:
- **Dashboard**: http://localhost:3000
- **Broker API**: http://localhost:8080/api/stats

### Local Development

**Prerequisites**: Go 1.22+, Node.js 20+

```bash
# Terminal 1: Run the broker
cd broker
go run .

# Terminal 2: Run the dashboard
cd dashboard
npm install
npm run dev
```

## Testing the Broker

### Create a topic
```bash
curl -X POST http://localhost:8080/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "partitions": 3}'
```

### Produce messages
```bash
curl -X POST http://localhost:8080/api/produce \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "key": "user-42", "value": "order placed"}'
```

### View topics
```bash
curl http://localhost:8080/api/topics | jq
```

### View messages
```bash
curl "http://localhost:8080/api/messages/orders/0?offset=0&limit=10" | jq
```

### View broker stats
```bash
curl http://localhost:8080/api/stats | jq
```

### Smoke test (via Makefile)
```bash
make smoke-test
```

## Project Structure

```
mini-kafka/
├── broker/                     # Go broker
│   ├── main.go                 # Entry point, graceful shutdown
│   ├── go.mod
│   ├── storage/
│   │   ├── log.go              # CommitLog — manages ordered segments
│   │   ├── segment.go          # Segment — single .log + .index pair
│   │   ├── index.go            # Index — offset → byte position mapping
│   │   ├── mmap_unix.go        # Real mmap via syscall (Linux/macOS)
│   │   └── mmap_windows.go     # Buffered I/O fallback (Windows)
│   ├── topic/
│   │   ├── topic.go            # Topic manager + registry
│   │   └── partition.go        # Partition with commit log
│   ├── consumer/
│   │   └── group.go            # Consumer group offset tracking
│   ├── network/
│   │   ├── protocol.go         # Binary wire protocol definitions
│   │   ├── server.go           # TCP server
│   │   └── transfer.go         # Zero-copy file transfer
│   ├── api/
│   │   └── http.go             # REST API for dashboard
│   └── Dockerfile
├── dashboard/                  # Next.js frontend
│   ├── src/app/
│   │   ├── layout.tsx          # Root layout (Inter font, dark theme)
│   │   ├── page.tsx            # Main dashboard page
│   │   ├── globals.css         # Tailwind v4 + design tokens
│   │   └── components/
│   │       ├── StatCard.tsx     # Metric display card
│   │       ├── TopicTable.tsx   # Topic listing table
│   │       ├── MessageStream.tsx# Live message feed
│   │       └── ConsumerGroups.tsx
│   ├── package.json
│   ├── postcss.config.mjs
│   └── Dockerfile
├── docker-compose.yml          # Single-command deployment
├── Makefile                    # Build, run, test, smoke-test
└── README.md
```

## Binary Wire Protocol

The TCP protocol uses a compact binary framing:

```
Request:  [total_len:u32][api_key:u16][correlation_id:u32][payload...]
Response: [total_len:u32][correlation_id:u32][error_code:i16][payload...]
```

| API Key | Operation      | Description                      |
|---------|---------------|----------------------------------|
| 0       | PRODUCE       | Append message to topic          |
| 1       | FETCH         | Read messages from partition     |
| 2       | CREATE_TOPIC  | Create topic with N partitions   |
| 3       | LIST_TOPICS   | List all topics with stats       |
| 4       | COMMIT_OFFSET | Commit consumer group offset     |
| 5       | FETCH_OFFSET  | Get consumer group offset        |

## On-Disk Format

### Message Layout (`.log` files)
```
[total_len:u32][timestamp:i64][key_len:u16][key:bytes][val_len:u32][value:bytes]
```

### Index Entry Layout (`.index` files)
```
[relative_offset:u32][byte_position:u32]    (8 bytes per entry)
```

### Directory Structure
```
data/
├── my-topic/
│   ├── partition-0/
│   │   ├── 00000000000000000000.log
│   │   └── 00000000000000000000.index
│   └── partition-1/
│       ├── 00000000000000000000.log
│       └── 00000000000000000000.index
└── .offsets/
    └── my-consumer-group.json
```

## Configuration

| Environment Variable | Default     | Description                |
|---------------------|-------------|----------------------------|
| `DATA_DIR`          | `./data`    | Log data directory         |
| `TCP_ADDR`          | `:9092`     | TCP protocol listen address|
| `HTTP_ADDR`         | `:8080`     | HTTP API listen address    |
| `NEXT_PUBLIC_API_URL`| `http://localhost:8080` | Broker API URL (dashboard) |

## License

MIT
