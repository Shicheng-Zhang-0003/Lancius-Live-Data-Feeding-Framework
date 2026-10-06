#ifndef SNAPSHOT_H
#define SNAPSHOT_H

/*
 * LDFD - Live Data Feed Daemon
 * Copyright (C) 2026
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations (keeps cJSON/curl specifics out of this header).
 *
 * Note curl/curl.h is deliberately NOT included: libcurl typedefs CURLM as
 * `void`, so it cannot be forward-declared as an opaque struct type without
 * colliding. Including it unconditionally meant every consumer of the buffer
 * or the parsers -- none of which touch HTTP -- still needed libcurl headers
 * installed. multi_handle is therefore void* here and cast inside fetch.c. */
struct cJSON;

/* ============================================================================
 * Core Types
 * ============================================================================ */

typedef enum {
    SNAP_FMT_CSV,
    SNAP_FMT_JSON,
    SNAP_FMT_NDJSON,
    SNAP_FMT_BINARY,
    SNAP_FMT_GEOTIFF,
    SNAP_FMT_NETCDF
} snap_format_t;

typedef enum {
    SNAP_OK = 0,
    SNAP_ERR_NOMEM,
    SNAP_ERR_CURL,
    SNAP_ERR_PARSE,
    SNAP_ERR_TRANSFORM,
    SNAP_ERR_OUTPUT,
    SNAP_ERR_CONFIG
} snap_error_t;

typedef struct snap_buffer {
    uint8_t *data;
    size_t len;
    size_t cap;
} snap_buffer_t;

typedef struct snap_chunk {
    uint8_t *data;
    size_t len;
    int is_final;
} snap_chunk_t;

/* Stream parser callbacks - process data chunk-by-chunk, never store full dataset.
 * reset (optional): clear per-parse state for a fresh response while KEEPING
 * any callback the user registered. Recreating the whole ctx instead loses
 * the callback, which is why a re-polled source went silent. */
typedef struct snap_parser {
    void *ctx;
    snap_error_t (*init)(void **ctx, const char *config);
    snap_error_t (*feed)(void *ctx, const snap_chunk_t *chunk);
    snap_error_t (*flush)(void *ctx);
    void (*free)(void *ctx);
    snap_error_t (*reset)(void *ctx);
} snap_parser_t;

/* Transform: modify/filter/enrich streaming records */
typedef struct snap_transform {
    void *ctx;
    snap_error_t (*init)(void **ctx, const char *config);
    snap_error_t (*process)(void *ctx, const snap_chunk_t *in, snap_buffer_t *out);
    void (*free)(void *ctx);
} snap_transform_t;

/* Output handler: upload/forward processed data */
typedef struct snap_output {
    void *ctx;
    snap_error_t (*init)(void **ctx, const char *config);
    snap_error_t (*write)(void *ctx, const snap_chunk_t *chunk);
    snap_error_t (*close)(void *ctx);
    void (*free)(void *ctx);
} snap_output_t;

/* Data source definition */
typedef struct snap_source {
    char *name;
    char *url;
    char *auth_header;      /* Bearer token, API key, etc. */
    snap_format_t format;
    int interval_sec;       /* 0 = one-shot */
    char *parser_config;
    char *transform_config;
    char *output_config;
    struct snap_source *next;
} snap_source_t;

/* Pipeline: source -> parser -> transform(s) -> output */
typedef struct snap_pipeline {
    snap_source_t *source;
    snap_parser_t parser;
    snap_transform_t *transforms;
    int n_transforms;
    snap_output_t output;
    int running;       /* deprecated: scheduling now lives on snap_ctx_t */
    pthread_t thread;  /* deprecated: use snap_ctx_t.loop_thread */
} snap_pipeline_t;

/* Global framework context */
typedef struct snap_ctx {
    snap_pipeline_t *pipelines;
    int n_pipelines;
    void *multi_handle;   /* CURLM*, opaque here; see note above */
    int shutdown;
    int running;             /* async loop active (set by snap_ctx_start) */
    pthread_t loop_thread;   /* background curl_multi loop */
    int loop_thread_valid;
    /* Live fetch tasks owned by the async loop. Tracked so an early exit can
     * detach every still-running transfer before curl_multi_cleanup. */
    struct fetch_task **tasks;
    int n_tasks;
    /* Scheduler: one wakeup deadline per pipeline (monotonic seconds).
     * Zero interval means one-shot (never rescheduled). */
    double *next_due;
} snap_ctx_t;

/* ============================================================================
 * Buffer Utilities (zero-copy where possible)
 * ============================================================================ */

void snap_buffer_init(snap_buffer_t *buf, size_t initial_cap);
void snap_buffer_append(snap_buffer_t *buf, const uint8_t *data, size_t len);
void snap_buffer_reset(snap_buffer_t *buf);
void snap_buffer_free(snap_buffer_t *buf);

/* ============================================================================
 * Framework API
 * ============================================================================ */

snap_ctx_t *snap_ctx_new(void);
void snap_ctx_free(snap_ctx_t *ctx);

snap_error_t snap_ctx_add_source(snap_ctx_t *ctx, const snap_source_t *src);
snap_error_t snap_ctx_start(snap_ctx_t *ctx);
snap_error_t snap_ctx_stop(snap_ctx_t *ctx);
snap_error_t snap_ctx_run_once(snap_ctx_t *ctx);  /* Single snapshot all sources */
/* Rebuild a pipeline's parser for a fresh poll. Required between polls: a
 * reused parser keeps header_done set and replays the CSV header as data. */
snap_error_t snap_pipeline_rearm(snap_pipeline_t *p);
snap_error_t snap_ctx_load_config(snap_ctx_t *ctx, const char *config_file);
/* Parse one {"name","url",...} source object (cJSON kept forward-declared). */
snap_source_t *snap_source_from_json(struct cJSON *obj);

/* One-shot fetch: stream URL through parser (declared here so
 * context.c / examples need no private fetch_task_t knowledge). */
snap_error_t snap_fetch_once(const char *url, const char *auth_header,
                             snap_parser_t *parser, void *parser_ctx);

/* One-shot fetch of the raw body into a caller-owned buffer. This is the
 * entry point for language bridges (see ldfd_bridge.py): bytes, not
 * parsed records, with no callback on the hot path.
 * `out` is reused, not reallocated. max_bytes caps the size (0 = no cap);
 * exceeding it returns SNAP_ERR_OUTPUT instead of truncating silently.
 * An HTTP status >= 400 is reported as SNAP_ERR_CURL, so a saved error
 * page is never mistaken for a dataset. */
snap_error_t snap_fetch_to_buffer(const char *url, const char *auth_header,
                                  snap_buffer_t *out, size_t max_bytes);

/* ============================================================================
 * Built-in Parsers
 * ============================================================================ */

extern const snap_parser_t snap_csv_parser;
extern const snap_parser_t snap_json_parser;
extern const snap_parser_t snap_ndjson_parser;

/* JSON object callback (cJSON forward-declared so <cjson/cJSON.h> is only
 * needed by the .c file and by users that actually consume objects). */
void snap_json_set_callback(void *parser_ctx,
                            void (*cb)(struct cJSON *obj, void *udata),
                            void *udata);

/* ============================================================================
 * Built-in Outputs
 *
 * Only the sinks that exist are declared. kafka / s3 / http and the
 * filter / project / enrich transforms were previously declared here with
 * no definition anywhere in the tree: any program following the readme's
 * transform and output tables failed to link. They are documented as
 * roadmap in readme.md, not advertised as available API.
 * ============================================================================ */

extern const snap_output_t snap_file_output;     /* append bytes to a file */
extern const snap_output_t snap_callback_output; /* hand chunks to a C callback */

#ifdef __cplusplus
}
#endif

#endif /* SNAPSHOT_H */