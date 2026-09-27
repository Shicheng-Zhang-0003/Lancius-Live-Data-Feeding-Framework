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
#include <curl/curl.h>

#ifdef __cplusplus
extern "C" {
#endif

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

/* Stream parser callbacks - process data chunk-by-chunk, never store full dataset */
typedef struct snap_parser {
    void *ctx;
    snap_error_t (*init)(void **ctx, const char *config);
    snap_error_t (*feed)(void *ctx, const snap_chunk_t *chunk);
    snap_error_t (*flush)(void *ctx);
    void (*free)(void *ctx);
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
    int running;
    pthread_t thread;
} snap_pipeline_t;

/* Global framework context */
typedef struct snap_ctx {
    snap_pipeline_t *pipelines;
    int n_pipelines;
    CURLM *multi_handle;
    int shutdown;
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

/* ============================================================================
 * Built-in Parsers
 * ============================================================================ */

extern const snap_parser_t snap_csv_parser;
extern const snap_parser_t snap_json_parser;
extern const snap_parser_t snap_ndjson_parser;

/* ============================================================================
 * Built-in Transforms
 * ============================================================================ */

extern const snap_transform_t snap_filter_transform;   /* Filter rows by condition */
extern const snap_transform_t snap_project_transform;  /* Select/rename columns */
extern const snap_transform_t snap_enrich_transform;   /* Join with static data */

/* ============================================================================
 * Built-in Outputs
 * ============================================================================ */

extern const snap_output_t snap_http_output;    /* POST to HTTP endpoint */
extern const snap_output_t snap_kafka_output;   /* Produce to Kafka */
extern const snap_output_t snap_s3_output;      /* PUT to S3-compatible */
extern const snap_output_t snap_callback_output;/* User callback function */

#ifdef __cplusplus
}
#endif

#endif /* SNAPSHOT_H */