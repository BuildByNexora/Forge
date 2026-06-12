# Forge

Embedded job queue for Python. No Redis. No daemon. No broker.

Forge is a persistent, single-process job queue that embeds directly in your Python application. Jobs are stored durably via an append-only log (AOF) with SHA-256 checksums — same proven storage architecture as [Kron](https://github.com/BuildByNexora/Kron) (scheduler) and [Flint](https://github.com/BuildByNexora/Flint) (rate limiter).

```python
import forge

queue = forge.Queue(data_dir=".forge")

queue.push("send_email", payload={"to": "user@example.com"}, priority=1)
queue.push("resize_image", payload={"path": "/tmp/x.png"}, delay="10s")

@queue.worker("send_email")
def handle_email(payload, context):
    send(payload["to"])

queue.run()
```

## Features (v0.1)

- **Persistent queue** — survives restarts via AOF + snapshot + compaction  
- **Priority scheduling** — higher number = higher priority  
- **Delayed jobs** — run after N seconds  
- **Retry with exponential backoff** — configurable max attempts  
- **Dead letter queue** — jobs that exceed max attempts are quarantined  
- **Job history** — full lifecycle per job (queued → claimed → succeeded/failed → retrying → dead)  
- **Crash recovery** — claimed-but-incomplete jobs are returned to the queue on restart  
- **Exclusive data directory locking** — single-writer semantics  
- **Integrity verified** — SHA-256 checksums on every AOF record and snapshot  
- **Zero external dependencies** — Rust core, no Redis, no system daemon  

## Installation

```bash
pip install forge
```

Or build from source:

```bash
pip install maturin
maturin develop
```

## Usage

### Basic

```python
import forge

queue = forge.Queue(data_dir=".forge")

# Push a job with priority (higher = more important)
queue.push("send_email", payload={"to": "user@example.com"}, priority=1)

# Push a delayed job (runs after delay)
queue.push("resize_image", payload={"path": "/tmp/x.png"}, delay="10s")
```

### Workers

```python
@queue.worker("send_email")
def handle(payload, context):
    print(f"Sending email to {payload['to']}")
    # context.job_id, context.attempt available
```

### Run

Blocking:
```python
queue.run()  # processes jobs forever
```

Non-blocking (background thread):
```python
queue.start()
# ... do other work ...
queue.stop()
```

### CLI

```bash
forge --data-dir .forge queue list
forge --data-dir .forge job status <job_id>
forge --data-dir .forge job history <job_id>
forge --data-dir .forge dead list
forge --data-dir .forge dead retry <job_id>
forge --data-dir .forge doctor
forge --data-dir .forge compact
```

## Storage

```
.forge/
├── forge.aof        # Append-only log (AOF)
├── forge.snapshot   # Compressed state snapshot
└── forge.lock       # Exclusive data directory lock
```

### AOF events

| Event | Description |
|-------|-------------|
| `JOB_PUSHED` | A new job was enqueued |
| `JOB_CLAIMED` | A worker claimed the job |
| `JOB_SUCCEEDED` | Job completed successfully |
| `JOB_FAILED` | Job failed (with error) |
| `JOB_RETRYING` | Job scheduled for retry |
| `JOB_DEAD` | Job moved to dead letter queue |
| `JOB_REQUEUED_AFTER_CRASH` | Job re-queued during crash recovery |

## Forge vs Celery

| Feature | Forge | Celery |
|---------|-------|--------|
| **Infrastructure** | None — embeds in your process | Requires Redis/RabbitMQ + worker daemon |
| **Installation** | `pip install forge` | `pip install celery` + run Redis + run celeryd |
| **Persistence** | Built-in AOF with SHA-256 checksums | Delegates to broker (Redis with AOF, RabbitMQ) |
| **Dead letter queue** | Built-in, per-job | Requires broker-specific DLQ config |
| **Job history** | Built-in per job_id lifecycle | Requires external monitoring (Flower) |
| **Crash recovery** | Automatic on restart | Depends on broker config |
| **Priority** | Built-in (numeric priority) | Supported but broker-dependent |
| **Delayed jobs** | Built-in (delay="10s") | Via countdown/ETA, needs Redis |
| **Retry** | Built-in exponential backoff | Supported, requires config |
| **Configuration** | Zero — single data_dir param | Multiple settings, broker URL, result backend |
| **Dependencies** | Zero external runtime deps | Redis or RabbitMQ, celeryd process |
| **Data integrity** | SHA-256 checksums on every record | Depends on broker guarantees |
| **Deployment** | Single `python app.py` | app.py + celeryd + Redis + monitoring |

Forge is ideal for applications that want a durable job queue without operating a distributed infrastructure. If you need multi-node workers, task routing, or massive throughput, use Celery. If you want reliability without complexity, use Forge.

## Development

```bash
cargo build
cargo test
```

The project uses the same architectural patterns as Kron and Flint:

- **AOF** — Append-only log with NDJSON format
- **Snapshot/compaction** — Atomic `.tmp` → fsync → rename → fsync dir
- **Checksums** — SHA-256 with constant-time comparison
- **Locking** — BSD flock via `fs2`
- **PyO3** — GIL released during blocking Rust calls

## License

BSD-3-Clause
