# The Lancius Live Data Feeding Framework

**A zero-copy, streaming framework for extracting real-time web data into applications.**

> **Implementation status (read before the spec below).** This readme
> describes the full vision *plus* what exists today. Actually implemented:
> growable buffers (`src/buffer.c`), one-shot + async HTTP fetch with a real
> `interval_sec` scheduler (`src/fetch.c` — `snap_fetch_once`,
> `snap_fetch_to_buffer`, `snap_ctx_start/stop/run_once`), header-aware
> streaming CSV parsing with per-response `reset` (`src/parser_csv.c`),
> buffered JSON-array + line-split NDJSON (`src/parser_json.c`), JSON
> declarative source loading (`src/config.c`), the ecological
> associated-data module (`include/assoc.h`, `src/assoc.c` — FIRMS/Daymet/
> GBIF, libc-only), **output sinks** (`src/output.c` — file, callback, and
> CSV-rows-to-JSONL), and **decompression** (`src/decompress.c` — streaming
> gzip plus a tar reader with tar-slip guards). Three examples build.
>
> **Roadmap, not built:** Kafka/S3/HTTP sinks, websocket and SSE sources,
> parquet/protobuf/msgpack parsers, checkpointing, metrics, schema
> registry, and everything under "Universal Data Pipeline v2" below. The
> `filter`/`project`/`enrich` transforms and the kafka/s3/http outputs were
> previously *declared in the header with no definition anywhere*; those
> declarations have been deleted rather than left to fail at link time.
> Performance figures (~50KB, 100MB/s, <1ms, 50+ streams) are design
> targets, not benchmarks. Treat the v2 sections as an architecture RFC
> and the file list below as the buildable truth.

## Testing

```bash
make test         # four dependency-free suites (libc + zlib), ~92 checks
make test-fetch   # fetch/scheduler suite; needs libcurl headers, else SKIP
make strict       # -Wconversion syntax check
make analyze      # gcc -fanalyzer
make sanitize     # ASan + UBSan over the dependency-free suites
```

`make test` never touches the network and exits non-zero on any failure.
`make test-fetch` binds a loopback HTTP server and only ever dials
127.0.0.1; without libcurl development headers it prints a SKIP rather than
reporting a pass.

## Building

```bash
# System dependencies (headers required, not just runtime libs)
# Ubuntu/Debian: apt-get install build-essential pkg-config libcurl4-openssl-dev libcjson-dev
# RHEL/Fedora:   dnf install gcc make pkgconf libcurl-devel cjson-devel
# macOS:         brew install curl cjson pkg-config

make              # C17, pkg-config cflags/libs: examples + libsnapshot.so
make test         # network-free assoc suite (libc only)
make strict       # -Wconversion syntax check over src + examples
make analyze      # gcc -fanalyzer syntax check
make sanitize     # ASan+UBSan rebuild + test
make install      # installs snapshot.h + assoc.h + parser_csv.h + libsnapshot.so
```

Toolchain: `-std=c17`, `-Wall -Wextra -Wpedantic -Wshadow` clean under both
gcc 15 and clang 21; verified with `libcurl 8.18.0 + cJSON 1.7.19`.
`valgrind --leak-check=full` is clean on the assoc and core harnesses
(the 56-byte still-reachable block is `curl_global_init`, freed at exit).

The associated-data module and its test need **libc only**:

```bash
gcc -std=c17 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude \
    tests/test_assoc.c src/assoc.c -o /tmp/test_assoc && /tmp/test_assoc
```

## Repository Map

```
3463-LDFD/
├── include/snapshot.h     # core types: buffer/chunk, parser/transform/output
│                          #   vtables, source/pipeline/context, framework API.
│                          #   Deliberately does NOT include <curl/curl.h>.
├── include/parser_csv.h   # csv_row_cb_t + snap_csv_set_callback()
├── include/output.h       # file / callback / row-to-JSONL sinks
├── include/decompress.h   # streaming gzip + tar
├── include/assoc.h        # associated-data module (FIRMS/Daymet/GBIF, Idea1 feeds)
├── src/buffer.c           # growable reusable byte buffer
├── src/context.c          # context lifecycle, source registration, run-once
├── src/fetch.c            # libcurl one-shot fetch, buffer fetch, async loop,
│                          #   interval scheduler, redirect/protocol hardening
├── src/parser_csv.c       # streaming CSV (quoted fields, delimiter, skip=N)
├── src/parser_json.c      # buffered JSON array + NDJSON, per-response reset
├── src/config.c           # snap_ctx_load_config() from declarative JSON
├── src/output.c           # output sinks (file / callback / CSV-rows-to-JSONL)
├── src/decompress.c       # zlib streaming inflate + tar extraction
├── src/assoc.c            # associated-data module (see dedicated section below)
├── examples/fire_monitor.c      # continuous FIRMS poll on the scheduler
├── examples/multi_pipeline.c    # two sources on independent intervals
├── examples/poll_once.c         # fetch once -> file, inflating gzip / untar
├── examples/pipeline_config.json# 4-source declarative config (FIRMS/GBIF/Daymet)
└── tests/
    ├── test_assoc.c          # 4 network-free assoc scenarios
    ├── test_assoc_regress.c  # regressions for 9 defects found in audit
    ├── test_core.c           # buffer, CSV edge cases, outputs (no curl)
    ├── test_decompress.c     # gzip + tar incl. tar-slip, truncation
    └── test_fetch.c          # fetch/scheduler vs a loopback server (needs curl)
```

The Lancius Live Data Feeding Framework is a lightweight C library for building continuous data pipelines that fetch, parse, transform, and route live HTTP data streams — without ever writing to disk.

Modern applications need live data: market ticks, sensor readings, API feeds, public datasets. Traditional approaches download entire files, parse them, then process — wasting memory, latency, and bandwidth.

LLDFF streams data **chunk-by-chunk** through a composable pipeline:

```
HTTP Source → Streaming Parser → Transforms → Output Sink
     │              │               │            │
   libcurl        CSV/JSON       Filter/      HTTP/Kafka/
   (async)        (line-by-line)  Project/     S3/Callback
                               Enrich
```

## Features

- **Streaming** — Data flows through reusable buffers; no full-dataset allocation.
  (Note: the CSV parser currently appends one byte at a time, so the
  "zero-copy" and 100MB/s figures below are aspirational, not measured.)
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
| CSV | `snap_csv_parser` | Quoted fields, CRLF-safe, `parser_config="delimiter=;skip=6"` (Daymet preamble); register rows with `snap_csv_set_callback(ctx, cb, udata)` |
| JSON | `snap_json_parser` | Buffers array/object documents (chunk-split safe), emits each element/object via `snap_json_set_callback`; parsed on `flush` |
| NDJSON | `snap_ndjson_parser` | One JSON object per line (split-chunk safe, unterminated last line flushed) |

> For FIRMS/Daymet/GBIF ecological feeds prefer the libc-only `assoc` module
> below: it does header-name lookup and preamble handling without cJSON.

## Built-in Transforms (ROADMAP — not implemented)

None of these exist; `snap_transform_t` has no built-in instances and the
earlier header declarations were removed. Configure via `transform_config`
when they land:

| Transform | Syntax | Example |
|-----------|--------|---------|
| Filter | `filter=expr` | `filter=frp>100;confidence>80` |
| Project | `project=cols` | `project=lat,lon,frp,timestamp` |
| Enrich | `enrich=file` | `enrich=static_lookup.csv` |

Expressions support: `> < >= <= == !=`, `&& ||`, parentheses.

## Built-in Outputs

Two exist (`snap_file_output`, `snap_callback_output`, plus the
CSV-rows-to-JSONL row sink in `include/output.h`). The rest below are
roadmap.

| Output | Config Format | Description |
|--------|---------------|-------------|
| File | *(built)* | append raw bytes to a path, atomically |
| Callback | *(built)* | hand each chunk to a C function |
| HTTP | `http://host:port/path` | POST JSON batches — ROADMAP |
| Kafka | `kafka://broker:9092/topic` | Produce to topic — ROADMAP |
| S3 | `s3://bucket/prefix` | PUT objects (multipart) — ROADMAP |

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

- **Throughput**: 100MB/s+ on modest hardware (network-bound) — TARGET, not measured
- **Latency**: <1ms parser-to-output for CSV/JSON — TARGET, not measured
- **Memory**: Fixed ~2MB base + configurable buffer pool — TARGET
- **Concurrency**: 50+ simultaneous HTTP streams per context — the async
  loop is a single `curl_multi` thread and has only been exercised with a
  handful of sources

## Use Cases

- **IoT telemetry ingestion** — Stream sensor data to time-series DB
- **Financial market data** — Normalize multi-exchange feeds
- **Public dataset monitoring** — Track changes in government/open data portals
- **Log aggregation** — Tail remote logs via HTTP range requests
- **ML feature pipelines** — Stream features to online inference

## Building from Source

```bash
# Dependencies
# Ubuntu/Debian: apt-get install build-essential pkg-config libcurl4-openssl-dev libcjson-dev
# RHEL/Fedora: dnf install gcc make pkgconf libcurl-devel cjson-devel
# macOS: brew install curl cjson pkg-config

make              # Build examples + library (C17 + pkg-config)
make test         # Assoc suite (no network)
make install      # System install (requires root)
```

## Linking in Your Project

```bash
# Shared (build it first: make -C 3463-LDFD)
gcc -std=c17 -I3463-LDFD/include your_app.c \
    -L3463-LDFD -lsnapshot -lcurl -lz -lpthread -o your_app

From Python, prefer the ctypes bridge rather than hand-rolled ctypes:
see ../ldfd_bridge.py, which exposes only buffer-in/buffer-out entry
points so nothing is ever called back into Python from C.
```

## License

GPLv3 — See `LICENSE` for terms. Commercial licensing available.

## Contributing

Internal project — see `CONTRIBUTING.md` for workflow.

---

# Universal Data Pipeline v2 — Extended Architecture

## Complete Pipeline Components

### Sources (11 types)
| Source | Config | Description |
|--------|--------|-------------|
| HTTP/REST | `url, auth, method, headers, params` | Poll REST APIs |
| WebSocket | `url, protocol, subscribe_msg` | Real-time streams |
| Server-Sent Events | `url, reconnect_interval` | SSE feeds |
| S3/GCS | `bucket, prefix, region, creds` | Object storage listing |
| Git | `url, ref, paths, auth` | Raw file from repo |
| File Watch | `path, pattern, recursive` | Local file changes |
| Cron | `schedule, command` | Periodic shell commands |
| Custom | `init_cb, fetch_cb, close_cb` | Your own source |

### Parsers (12 types)
| Parser | Input Format | Output Chunks |
|--------|--------------|---------------|
| CSV | RFC4180 CSV/TSV | Row chunks (STR arrays) |
| JSON | JSON array/object | Object chunks (typed fields) |
| NDJSON | Newline-delimited JSON | One object per line |
| Parquet | Apache Parquet | Columnar batches |
| Protobuf | Protocol Buffers | Decoded messages |
| MessagePack | MessagePack | Decoded objects |
| Binary | Raw bytes + schema | Typed arrays/tensors |
| Tensor | Custom binary tensor | `SNAP_DTYPE_TENSOR` |
| Lancius v2 | `.lancius` model | Model chunks (weights, graph) |
| ONNX | `.onnx` model | Graph + weights |
| PyTorch | `.pt` tensor dict | Tensor dictionaries |
| TensorFlow | TFRecord | TensorFlow records |
| NumPy | `.npy`/`.npz` | NumPy arrays |

### Transforms (10 types)
| Transform | Config Syntax | Description |
|-----------|---------------|-------------|
| Filter | `filter=expr` | Keep chunks matching expression |
| Project | `project=field1,field2` | Select/rename fields |
| Map | `map=field:expr` | Compute new fields |
| Enrich | `enrich=source:key` | Join with static/reference data |
| Aggregate | `agg=field:op:window` | Sum/mean/min/max/count over window |
| Window | `window=tumbling:60s` | Tumbling/sliding/session windows |
| Normalize | `normalize=field:method` | Z-score, min-max, log, sqrt |
| Tokenize | `tokenize=field:vocab` | Text → token IDs |
| Tensorize | `tensorize=fields:shape` | Row → tensor chunk |
| Batch | `batch=size:timeout` | Accumulate N chunks or timeout |

### Outputs (11 types)
| Output | Config | Description |
|--------|--------|-------------|
| HTTP | `url, method, batch_size, format` | POST JSON/msgpack/protobuf |
| Kafka | `brokers, topic, partition_key` | Produce to topic |
| S3/GCS | `bucket, prefix, format, part_size` | Multipart upload |
| Redis | `url, key_pattern, ttl` | Streams/Lists/Hashes |
| PostgreSQL | `dsn, table, batch_size` | COPY/INSERT batches |
| ClickHouse | `url, table, format` | Native protocol |
| Lancius v2 | `path, format=v2` | Write `.lancius` model |
| ONNX | `path` | Write `.onnx` model |
| PyTorch | `path, format=pt` | Write `.pt` tensor dict |
| TensorFlow | `path, format=tfrecord` | Write TFRecord |
| NumPy | `path, format=npy` | Write `.npy`/`.npz` |
| Callback | `func_name` | Call C function |
| Custom | `write_cb, close_cb` | Your sink |

## Declarative Configuration

Define entire pipeline networks in JSON:

```json
{
  "feeds": [
    {
      "name": "lancius-training-corpus",
      "source": {
        "type": "http",
        "url": "https://datasets.example.com/shards/{shard}.jsonl",
        "params": {"shard": "0..99"},
        "interval_sec": 3600
      },
      "parser": "ndjson",
      "transforms": [
        {"type": "filter", "config": "filter=text_len>50 && lang==en"},
        {"type": "tokenize", "config": "tokenize=text:vocab_32k.json"},
        {"type": "tensorize", "config": "tensorize=tokens:[seq_len]"},
        {"type": "batch", "config": "batch=2048:30s"}
      ],
      "output": {
        "type": "lancius_v2",
        "config": "path=/models/lancius/train_shard_{shard}.lancius"
      }
    },
    {
      "name": "realtime-sensor-feed",
      "source": {
        "type": "websocket",
        "url": "wss://sensors.example.com/stream",
        "subscribe_msg": "{\"channels\":[\"temp\",\"humidity\",\"pressure\"]}"
      },
      "parser": "json",
      "transforms": [
        {"type": "normalize", "config": "normalize=temp:zscore,humidity:minmax"},
        {"type": "window", "config": "window=tumbling:60s"},
        {"type": "aggregate", "config": "agg=temp:mean:60s,humidity:mean:60s"}
      ],
      "output": {
        "type": "kafka",
        "config": "brokers=kafka:9092,topic=sensor-aggregated"
      }
    }
  ]
}
```

Load and run:
```c
snap_ctx_t *ctx = snap_ctx_new();
snap_ctx_load_config(ctx, "pipelines.json");
snap_ctx_start(ctx);  // Runs all feeds concurrently
```

## Embedding in Your Application

### Minimal Example: Feed Data to Your ML Model

```c
#include "snapshot.h"

void on_tensor_chunk(snap_chunk_t *chunk, void *udata) {
    MyModel *model = udata;
    if (chunk->dtype == SNAP_DTYPE_F32 && chunk->ndim == 2) {
        float *input = chunk->data;
        int batch = chunk->shape[0];
        int features = chunk->shape[1];
        model_infer(model, input, batch, features);
    }
}

int main() {
    snap_ctx_t *ctx = snap_ctx_new();
    
    snap_feed_t feed = {
        .name = "inference-feed",
        .source = {
            .type = SNAP_SRC_HTTP,
            .url = "https://api.myservice.com/features?batch=100",
            .interval_sec = 10
        },
        .parser = &snap_json_parser,
        .transforms = (snap_transform_t[]){
            snap_project_transform,  // .config = "project=feature_1..feature_128"
            snap_tensorize_transform // .config = "tensorize=feature_*:[batch,128]"
        },
        .n_transforms = 2,
        .output = &snap_callback_output,
        .output_config = "on_tensor_chunk"
    };
    
    MyModel model = model_load("model.lancius");
    snap_ctx_add_feed(ctx, &feed, on_tensor_chunk, &model);
    snap_ctx_start(ctx);
    
    while (running) { ... }
    
    snap_ctx_free(ctx);
    return 0;
}
```

### Register Custom Components

```c
// Custom parser for proprietary format
static snap_error_t my_parse_init(void **ctx, const char *config) { ... }
static snap_error_t my_parse_feed(void *ctx, const snap_chunk_t *raw, snap_chunk_t **out, int *n_out) { ... }

const snap_parser_t my_parser = {
    .init = my_parse_init,
    .feed = my_parse_feed,
    .flush = my_parse_flush,
    .free = my_parse_free
};

// Register globally
snap_register_parser("myformat", &my_parser);

// Now use in config: "parser": "myformat"
```

## Integration with Lancius (3344)

LDFD includes first-class support for the **Lancius v2 model format** and **training data preparation**:

### Output Training Data for Lancius

```json
{
  "name": "lancius-pretrain",
  "source": { "type": "s3", "bucket": "my-corpus", "prefix": "commoncrawl/" },
  "parser": "parquet",
  "transforms": [
    { "type": "filter", "config": "filter=token_count>512 && quality>0.8" },
    { "type": "tokenize", "config": "tokenize=text:lancius_vocab_32k.json" },
    { "type": "tensorize", "config": "tensorize=input_ids:[2048]" },
    { "type": "batch", "config": "batch=4096:60s" }
  ],
  "output": { "type": "lancius_v2", "config": "path=/models/lancius/train_{date}.lancius" }
}
```

### Stream Inference Requests to Lancius Runtime

```c
// LDFD pulls inference requests → transforms → feeds Lancius executor
snap_feed_t inference_feed = {
    .source = { .type = SNAP_SRC_HTTP, .url = "https://api.app.com/inference-queue", .interval_sec = 1 },
    .parser = &snap_json_parser,
    .transforms = (snap_transform_t[]){
        snap_project_transform,   // .config = "project=prompt,params"
        snap_tokenize_transform,  // .config = "tokenize=prompt:lancius_vocab.json"
        snap_tensorize_transform  // .config = "tensorize=input_ids:[1,seq_len]"
    },
    .n_transforms = 3,
    .output = &snap_callback_output,
    .output_config = "lancius_execute"
};

void lancius_execute(snap_chunk_t *chunk, void *udata) {
    LanciusContext *lc = udata;
    // chunk->data is float* [1, seq_len] token IDs
    lancius_infer(lc, chunk->data, chunk->shape);
}
```

### Pull Model Weights from HuggingFace/ONNX → Lancius

```json
{
  "name": "model-sync",
  "source": { "type": "http", "url": "https://huggingface.co/org/model/resolve/main/model.onnx" },
  "parser": "onnx",
  "transforms": [
    { "type": "convert", "config": "convert=onnx_to_lancius_v2" }
  ],
  "output": { "type": "lancius_v2", "config": "path=/models/lancius/model.lancius" }
}
```

## Advanced Features

### Exactly-Once Processing
- Checkpointing via `snap_checkpoint_t` (offsets, cursor positions)
- Idempotent writes with deduplication keys
- Transactional output batches

### Backpressure & Flow Control
- Buffer pool with high/low watermarks
- Source throttling via `curl_multi` socket callbacks
- Transform/output blocking with configurable timeouts

### Schema Registry
- Register Avro/Protobuf/JSON schemas
- Automatic schema evolution (backward/forward compatibility)
- Validation at parser/transform boundaries

### Metrics & Observability
```c
snap_metrics_t *m = snap_ctx_get_metrics(ctx);
printf("Feeds: %d | Chunks/sec: %.0f | Latency p99: %.1fms | Errors: %llu\n",
       m->active_feeds, m->chunks_per_sec, m->latency_p99_ms, m->error_count);
```
Prometheus `/metrics` endpoint built-in.

### Horizontal Scaling
- Partition feeds by `shard_key` in config
- Run multiple LDFD instances with same config + different `shard_id`
- Automatic partition assignment via Redis/ZooKeeper/etcd

## API Reference

### Context
```c
snap_ctx_t *snap_ctx_new(void);
void snap_ctx_free(snap_ctx_t *ctx);
snap_error_t snap_ctx_add_feed(snap_ctx_t *ctx, const snap_feed_t *feed);
snap_error_t snap_ctx_load_config(snap_ctx_t *ctx, const char *json_path);
snap_error_t snap_ctx_start(snap_ctx_t *ctx);
snap_error_t snap_ctx_stop(snap_ctx_t *ctx);
snap_error_t snap_ctx_run_once(snap_ctx_t *ctx);
snap_metrics_t *snap_ctx_get_metrics(snap_ctx_t *ctx);
```

### Registration
```c
void snap_register_parser(const char *name, const snap_parser_t *parser);
void snap_register_transform(const char *name, const snap_transform_t *transform);
void snap_register_output(const char *name, const snap_output_t *output);
void snap_register_source(const char *name, const snap_source_vtable_t *vtable);
```

### Chunk Utilities
```c
snap_chunk_t *snap_chunk_new(snap_dtype_t dtype, size_t n_elements, size_t *shape, int ndim);
void snap_chunk_free(snap_chunk_t *chunk);
snap_chunk_t *snap_chunk_copy(const snap_chunk_t *src);
int snap_chunk_equals(const snap_chunk_t *a, const snap_chunk_t *b);
```

### Metadata
```c
snap_metadata_t *snap_meta_new(void);
void snap_meta_set(snap_metadata_t *m, const char *key, const char *value);
const char *snap_meta_get(const snap_metadata_t *m, const char *key);
void snap_meta_free(snap_metadata_t *m);
```

## Type System

| Type | Code | Use Case |
|------|------|----------|
| Float32 | `SNAP_DTYPE_F32` | ML tensors, embeddings |
| Float64 | `SNAP_DTYPE_F64` | High-precision numerics |
| Int32 | `SNAP_DTYPE_I32` | Indices, counts |
| Int64 | `SNAP_DTYPE_I64` | Timestamps, large counts |
| String | `SNAP_DTYPE_STR` | Text, categories |
| Bool | `SNAP_DTYPE_BOOL` | Masks, flags |
| Tensor | `SNAP_DTYPE_TENSOR` | Multi-dim arrays with shape |

## Related

- **Lancius (3344)** — C ML compiler/runtime this feeds: https://github.com/Shicheng-Zhang-0003/Lancius
- **BDC (243)** — Internal project using this framework

## Associated-Data Module (`include/assoc.h`, `src/assoc.c`)

Specialised, self-contained section for ecological "associated data"
(Idea1 protocol Sec 6–7): FIRMS active-fire CSV, Daymet single-pixel climate
CSV, and GBIF occurrence JSON. It owns its own line buffering, header
capture, and typed conversion, and deliberately does **not** sit on the
generic `snap_csv` parser (which consumes header rows internally, making
name-based lookup impossible through it) — no core files were changed, and
the module compiles on libc alone.

### Data model

```c
assoc_bbox_t    { lat_min, lat_max, lon_min, lon_max }   // NULL = no filter
assoc_fire_t    { lat, lon, brightness, frp, acq_date[16], satellite[16],
                  confidence_raw[16], confidence_num, confidence_class }
                // confidence_num >= 0 for numeric tokens ("88"), else -1;
                // confidence_class: 0 low, 1 nominal/n, 2 high/h, -1 unknown/numeric
assoc_climate_t { year, yday, dayl_s, prcp_mmd, srad_wm2, tmax_c, tmin_c,
                  vp_pa, has_swe, swe_mm }
assoc_occ_t     { species[64], lat, lon, uncert_m (-1 when absent),
                  event_date[32], basis[32] }
```

### API

| Function | Purpose |
|----------|---------|
| `assoc_phase1_bbox()` | Phase-1 study bbox matching `configs/params.yaml` (49.0–51.2N, 114.5–110.0W) |
| `assoc_in_bbox(bbox, lat, lon)` | Bounds check; `NULL` bbox always passes |
| `assoc_fire_feed_new(cb, udata, &opts)` | FIRMS feed; `opts = { min_frp (<0 disables), bbox (NULL disables) }` |
| `assoc_fire_feed(f, data, len)` | Push a raw chunk (call per `curl` write; split mid-row is fine) |
| `assoc_fire_feed_free(f)` | Flushes a trailing line without `\n`, then frees |
| `assoc_climate_feed_new(cb, udata)` / `assoc_climate_feed` / `assoc_climate_feed_free` | Same pattern for Daymet |
| `assoc_gbif_parse_page(json, len, species_label, cb, udata, bbox, max_uncert_m, &kept, &skipped)` | One GBIF `occurrence/search` page; returns emitted count or `-1` on malformed JSON; `max_uncert_m < 0` disables the uncertainty filter; records *without* uncertainty are always kept |

Header handling: the first line containing a known token becomes the header
(FIRMS: `latitude…frp` incl. VIIRS `bright_ti4`; Daymet: `year…vp`), earlier
lines (Daymet's 6-line preamble) are skipped, and lookup falls back to the
canonical positional order when a header is absent. Spaced Daymet headers
normalise (`prcp (mm/day)` → `prcp`). Quoted CSV fields are honoured.

### Worked example

```c
#include "assoc.h"
#include <stdio.h>

static void on_fire(const assoc_fire_t *f, void *udata) {
    (void)udata;
    printf("FIRE %.4f,%.4f FRP %.1f MW conf %s\n",
           f->lat, f->lon, f->frp, f->confidence_raw);
}

int main(void) {
    assoc_bbox_t bb = assoc_phase1_bbox();
    assoc_fire_opts_t opts = { 10.0, &bb };          // FRP>=10, Phase-1 bbox
    assoc_fire_feed_t *feed = assoc_fire_feed_new(on_fire, NULL, &opts);
    /* push each HTTP chunk as it arrives: */
    // assoc_fire_feed(feed, chunk_bytes, chunk_len);
    assoc_fire_feed_free(feed);                       // flush + free
    return 0;
}
```

### Tests

`tests/test_assoc.c` — 4 network-free scenarios built from the repo's real
header/row shapes: MODIS row incl. split-chunk streaming; VIIRS strings +
FRP/bbox filtering with unterminated last line; Daymet preamble + spaced
headers; GBIF bbox + uncertainty filtering. Build + run:

```bash
gcc -std=c17 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude \
    tests/test_assoc.c src/assoc.c -o /tmp/test_assoc && /tmp/test_assoc
# assoc tests: all pass
```

`tests/test_assoc_regress.c` pins the defects found in the v12R2
integration audit; it fails 15/20 checks against the pre-fix `assoc.c`.

The module is picked up by the normal `make` via the `src/*.c` wildcard, and
`make install` ships `assoc.h` alongside `snapshot.h`.