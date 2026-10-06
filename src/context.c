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

#include "snapshot.h"
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

static char *dup_or_null(const char *s) {
    if (!s) return NULL;
    char *d = strdup(s);
    return d;
}

static void snap_source_free_fields(snap_source_t *s) {
    if (!s) return;
    free(s->name);
    free(s->url);
    free(s->auth_header);
    free(s->parser_config);
    free(s->transform_config);
    free(s->output_config);
    free(s);
}

/* Despot truth: curl_global_init/cleanup used to be called once per context
 * (new -> init, free -> cleanup). With two live contexts, freeing one tore
 * down libcurl global state -- OpenSSL, proxy env, the resolver -- while the
 * survivor kept using it, which is undefined behaviour. init is now done
 * exactly once per process and cleanup is never called. */
static pthread_once_t g_curl_once = PTHREAD_ONCE_INIT;
static CURLcode g_curl_rc;

static void curl_global_init_once(void) {
    g_curl_rc = curl_global_init(CURL_GLOBAL_DEFAULT);
}

static void curl_ensure_init(void) {
    pthread_once(&g_curl_once, curl_global_init_once);
}

snap_ctx_t *snap_ctx_new(void) {
    curl_ensure_init();
    return calloc(1, sizeof(snap_ctx_t));
}

/* Reset a pipeline's parser for a fresh response.
 *
 * Two distinct problems, two distinct fixes:
 *  - Without a reset the CSV parser keeps header_done=1 from the previous
 *    poll and emits the header row as if it were data.
 *  - Freeing and re-initing the ctx instead drops any callback the user
 *    registered with snap_csv_set_callback(), so a re-polled source would
 *    go completely silent after the first response.
 * So prefer the parser's own reset (state only, callbacks kept); fall back
 * to free+init only when a parser has no reset. */
snap_error_t snap_pipeline_rearm(snap_pipeline_t *p) {
    if (!p) return SNAP_ERR_CONFIG;
    if (!p->parser.ctx) {
        if (p->parser.init) {
            return p->parser.init(&p->parser.ctx,
                                 p->source ? p->source->parser_config : NULL);
        }
        return SNAP_OK;
    }
    if (p->parser.reset) return p->parser.reset(p->parser.ctx);

    if (p->parser.free) p->parser.free(p->parser.ctx);
    p->parser.ctx = NULL;
    if (p->parser.init) {
        return p->parser.init(&p->parser.ctx,
                             p->source ? p->source->parser_config : NULL);
    }
    return SNAP_OK;
}

void snap_ctx_free(snap_ctx_t *ctx) {
    if (!ctx) return;
    snap_ctx_stop(ctx);
    if (ctx->loop_thread_valid) {
        pthread_join(ctx->loop_thread, NULL);
        ctx->loop_thread_valid = 0;
    }
    
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        if (p->source) snap_source_free_fields(p->source);
        if (p->parser.free && p->parser.ctx) p->parser.free(p->parser.ctx);
        if (p->transforms) {
            for (int j = 0; j < p->n_transforms; j++) {
                if (p->transforms[j].free && p->transforms[j].ctx)
                    p->transforms[j].free(p->transforms[j].ctx);
            }
            free(p->transforms);
        }
        if (p->output.free && p->output.ctx) p->output.free(p->output.ctx);
    }
    free(ctx->pipelines);
    free(ctx->tasks);
    free(ctx->next_due);

    if (ctx->multi_handle) curl_multi_cleanup((CURLM *)ctx->multi_handle);
    /* No curl_global_cleanup(): libcurl is initialised once per process and
     * must outlive any context. See curl_ensure_init(). */
    free(ctx);
}

snap_error_t snap_ctx_add_source(snap_ctx_t *ctx, const snap_source_t *src) {
    if (!ctx || !src || !src->name || !src->url) return SNAP_ERR_CONFIG;
    size_t want = ((size_t)ctx->n_pipelines + 1u) * sizeof(snap_pipeline_t);
    snap_pipeline_t *p = realloc(ctx->pipelines, want);
    if (!p) return SNAP_ERR_NOMEM;
    ctx->pipelines = p;

    snap_pipeline_t *pipe = &ctx->pipelines[ctx->n_pipelines];
    memset(pipe, 0, sizeof(snap_pipeline_t));

    pipe->source = calloc(1, sizeof(snap_source_t));
    if (!pipe->source) return SNAP_ERR_NOMEM;
    /* Deep-copy string fields individually (no struct memcpy: the source
     * struct's pointers are caller-owned and must not be aliased). */
    pipe->source->name = dup_or_null(src->name);
    pipe->source->url = dup_or_null(src->url);
    pipe->source->auth_header = dup_or_null(src->auth_header);
    pipe->source->parser_config = dup_or_null(src->parser_config);
    pipe->source->transform_config = dup_or_null(src->transform_config);
    pipe->source->output_config = dup_or_null(src->output_config);
    pipe->source->format = src->format;
    pipe->source->interval_sec = src->interval_sec;
    pipe->source->next = NULL;
    if (!pipe->source->name || !pipe->source->url) {
        free(pipe->source->name);
        free(pipe->source->url);
        free(pipe->source->auth_header);
        free(pipe->source->parser_config);
        free(pipe->source->transform_config);
        free(pipe->source->output_config);
        free(pipe->source);
        pipe->source = NULL;
        return SNAP_ERR_NOMEM;
    }

    /* Keep next_due in step with n_pipelines so the scheduler can arm a
     * source added after snap_ctx_start(). */
    if (ctx->running) {
        double *grown = realloc(ctx->next_due,
                                (size_t)(ctx->n_pipelines + 1) * sizeof(double));
        if (!grown) {
            snap_source_free_fields(pipe->source);
            return SNAP_ERR_NOMEM;
        }
        ctx->next_due = grown;
        ctx->next_due[ctx->n_pipelines] = 0.0;   /* due now */
    }

    switch (src->format) {
        case SNAP_FMT_CSV: pipe->parser = snap_csv_parser; break;
        case SNAP_FMT_JSON: pipe->parser = snap_json_parser; break;
        case SNAP_FMT_NDJSON: pipe->parser = snap_ndjson_parser; break;
        default: pipe->parser = snap_csv_parser;
    }

    if (pipe->parser.init) {
        snap_error_t rc = pipe->parser.init(&pipe->parser.ctx,
                                           pipe->source->parser_config);
        if (rc != SNAP_OK) {
            /* Despot truth: parser init failure used to be ignored, leaving
             * a pipeline with a NULL ctx that failed opaquely on first use. */
            snap_source_free_fields(pipe->source);
            return rc;
        }
    }

    ctx->n_pipelines++;
    return SNAP_OK;
}

snap_error_t snap_ctx_run_once(snap_ctx_t *ctx) {
    if (!ctx) return SNAP_ERR_CONFIG;
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        if (!p->source) continue;
        /* Each poll gets a fresh parser context: reusing one across polls
         * replayed the CSV header row as a data record on every call after
         * the first (header_done was never reset). */
        if (snap_pipeline_rearm(p) != SNAP_OK) return SNAP_ERR_PARSE;
        snap_error_t rc = snap_fetch_once(p->source->url, p->source->auth_header,
                                          &p->parser, p->parser.ctx);
        if (rc != SNAP_OK) return rc;
    }
    return SNAP_OK;
}