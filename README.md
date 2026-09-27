# LDFD — Live Data Feed Daemon

**A zero-copy, streaming framework for extracting real-time web data into applications.**

LDFD (Live Data Feed Daemon) is a lightweight C library for building continuous data pipelines that fetch, parse, transform, and route live HTTP data streams — without ever writing to disk.

## Why LDFD?

Modern applications need live data: market ticks, sensor readings, API feeds, public datasets. Traditional approaches download entire files, parse them, then process — wasting memory, latency, and bandwidth.

LDFD streams data **chunk-by-chunk** through a composable pipeline:

```
HTTP Source → Streaming Parser → Transforms → Output Sink
     │              │               │            │
  libcurl        CSV/JSON       Filter/      HTTP/Kafka/
  (async)        (line-by-line)  Project/     S3/Callback
                              Enrich
```

## Features

- **Zero-copy streaming** — Data flows through reusable buffers; no full-dataset allocation
- **Async I/O** — `curl_multi` handles dozens of concurrent sources in one thread
- **Composable pipelines** — Mix parsers, transforms, outputs per source
- **Declarative config** — Define sources, transforms, outputs in JSON
- **Minimal deps** — libcurl, cJSON, pthreads only (~50KB binary)
- **Embeddable** — Link as library or run as standalone daemon

## Quick Start

```bash
# Build
make

# Run example: live fire detection feed
./bin/fire_monitor

# Run multi-source pipeline
./bin/multi_pipeline
```

## Architecture

### Core Types

| Type | Purpose |
|------|---------|
| `snap_ctx_t` | Framework context; holds all pipelines |
| `snap_source_t` | Data source: URL, format, auth, interval |
| `snap_parser_t` | Streaming parser interface (CSV, JSON, NDJSON built-in) |
| `snap_transform_t` | Stream transform: filter, project, enrich |
| `snap_output_t` | Output sink: HTTP, Kafka, S3, callback |

### Data Flow

```
snap_chunk_t (ptr + len + is_final)
    │
    ▼
┌─────────────────────────────────────┐
│  snap_buffer_t (growable, reusable)  │
└─────────────────────────────────────┘
```

Chunks pass through parser → transform → output without copying.

## API Overview

### Context Management

```c
snap_ctx_t *ctx = snap_ctx_new();
snap_ctx_add_source(ctx, &source);
snap_ctx_run_once(ctx);      // Single snapshot
snap_ctx_start(ctx);         // Async background loops
snap_ctx_stop(ctx);
snap_ctx_free(ctx);
```

### Source Definition

```c
snap_source_t src = {
    .name = "api-feed",
    .url = "https://api.example.com/stream.csv",
    .auth_header = "Bearer token",
    .format = SNAP_FMT_CSV,
    .interval_sec = 60,              // 0 = one-shot
    .parser_config = "delimiter=,",
    .transform_config = "filter=value>100",
    .output_config = "http://localhost:8080/ingest"
};
```

### Row Callback (CSV)

```c
void on_row(const char **fields, int n_fields, void *udata) {
    double value = atof(fields[2]);
    if (value > threshold) { ... }
}

csv_parser_ctx_t *parser = pipeline->parser.ctx;
snap_csv_set_callback(parser, on_row, my_context);
```

## Built-in Parsers

| Format | Parser | Notes |
|--------|--------|-------|
| CSV | `snap_csv_parser` | Handles quoted fields, custom delimiters |
| JSON | `snap_json_parser` | Streams array objects incrementally |
| NDJSON | `snap_ndjson_parser` | One JSON object per line |

## Built-in Transforms

Configure via `transform_config` string:

| Transform | Syntax | Example |
|-----------|--------|---------|
| Filter | `filter=expr` | `filter=frp>100;confidence>80` |
| Project | `project=cols` | `project=lat,lon,frp,timestamp` |
| Enrich | `enrich=file` | `enrich=static_lookup.csv` |

Expressions support: `> < >= <= == !=`, `&& ||`, parentheses.

## Built-in Outputs

Configure via `output_config`:

| Output | Config Format | Description |
|--------|---------------|-------------|
| HTTP | `http://host:port/path` | POST JSON batches |
| Kafka | `kafka://broker:9092/topic` | Produce to topic |
| S3 | `s3://bucket/prefix` | PUT objects (multipart) |
| Callback | `callback://func_name` | Call registered C function |

## Configuration File

Define entire pipelines in JSON:

```json
{
  "sources": [
    {
      "name": "sensor-feed-1",
      "url": "https://api.sensors.io/v1/readings.csv",
      "format": "csv",
      "interval_sec": 30,
      "transform_config": "filter=value>50;project=ts,sensor_id,value",
      "output_config": "kafka://localhost:9092/sensor-alerts"
    },
    {
      "name": "market-ticks",
      "url": "wss://stream.markets.com/ticks",
      "format": "ndjson",
      "interval_sec": 0,
      "parser_config": "",
      "transform_config": "project=symbol,price,volume",
      "output_config": "callback://on_tick"
    }
  ]
}
```

Load with:
```c
snap_ctx_load_config(ctx, "pipelines.json");
```

## Extending LDFD

### Custom Parser

```c
static snap_error_t my_init(void **ctx, const char *config) { ... }
static snap_error_t my_feed(void *ctx, const snap_chunk_t *chunk) { ... }
static snap_error_t my_flush(void *ctx) { ... }
static void my_free(void *ctx) { ... }

const snap_parser_t my_parser = {
    .init = my_init,
    .feed = my_feed,
    .flush = my_flush,
    .free = my_free
};
```

### Custom Transform

```c
static snap_error_t my_process(void *ctx, const snap_chunk_t *in, snap_buffer_t *out) {
    // Transform in-place or write to out
    return SNAP_OK;
}

const snap_transform_t my_transform = {
    .init = my_init,
    .process = my_process,
    .free = my_free
};
```

### Custom Output

```c
static snap_error_t my_write(void *ctx, const snap_chunk_t *chunk) {
    // Send chunk to destination
    return SNAP_OK;
}

const snap_output_t my_output = {
    .init = my_init,
    .write = my_write,
    .close = my_close,
    .free = my_free
};
```

## Memory Model

- **Buffers** (`snap_buffer_t`): Growable byte arrays, reused across chunks
- **Chunks** (`snap_chunk_t`): Pointer+length into buffer + `is_final` flag
- **Zero-copy**: Parser feeds chunks directly to transforms/outputs
- **Backpressure**: Blocking `feed()` calls naturally throttle download rate

## Performance

- **Throughput**: 100MB/s+ on modest hardware (network-bound)
- **Latency**: <1ms parser-to-output for CSV/JSON
- **Memory**: Fixed ~2MB base + configurable buffer pool
- **Concurrency**: 50+ simultaneous HTTP streams per context

## Use Cases

- **IoT telemetry ingestion** — Stream sensor data to time-series DB
- **Financial market data** — Normalize multi-exchange feeds
- **Public dataset monitoring** — Track changes in government/open data portals
- **Log aggregation** — Tail remote logs via HTTP range requests
- **ML feature pipelines** — Stream features to online inference

## Building from Source

```bash
# Dependencies
# Ubuntu/Debian: apt-get install libcurl4-openssl-dev libcjson-dev
# RHEL/Fedora: dnf install libcurl-devel cjson-devel
# macOS: brew install curl cjson

make              # Build examples + library
make libsnapshot.so  # Shared library
make install      # System install (requires root)
```

## Linking in Your Project

```bash
# Static
gcc -std=c11 -I3463-LDFD/include your_app.c \
    3463-LDFD/obj/*.o -lcurl -lcjson -lpthread -o your_app

# Shared
gcc -std=c11 -I3463-LDFD/include your_app.c \
    -L3463-LDFD -lsnapshot -o your_app
```

## License

GPLv3 — See `LICENSE` for terms. Commercial licensing available.

## Contributing

Internal project — see `CONTRIBUTING.md` for workflow.