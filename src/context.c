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

snap_ctx_t *snap_ctx_new(void) {
    snap_ctx_t *ctx = calloc(1, sizeof(snap_ctx_t));
    curl_global_init(CURL_GLOBAL_DEFAULT);
    return ctx;
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
        if (p->source) {
            free(p->source->name);
            free(p->source->url);
            free(p->source->auth_header);
            free(p->source->parser_config);
            free(p->source->transform_config);
            free(p->source->output_config);
            free(p->source);
        }
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
    
    if (ctx->multi_handle) curl_multi_cleanup(ctx->multi_handle);
    curl_global_cleanup();
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
    
    switch (src->format) {
        case SNAP_FMT_CSV: pipe->parser = snap_csv_parser; break;
        case SNAP_FMT_JSON: pipe->parser = snap_json_parser; break;
        case SNAP_FMT_NDJSON: pipe->parser = snap_ndjson_parser; break;
        default: pipe->parser = snap_csv_parser;
    }
    
    if (pipe->parser.init) pipe->parser.init(&pipe->parser.ctx, pipe->source->parser_config);
    
    ctx->n_pipelines++;
    return SNAP_OK;
}

snap_error_t snap_ctx_run_once(snap_ctx_t *ctx) {
    if (!ctx) return SNAP_ERR_CONFIG;
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        if (!p->source) continue;
        /* interval_sec == 0 means one-shot; interval > 0 sources are
         * driven by snap_ctx_start()'s async loop or by the caller's
         * own scheduler. Run them here too so run_once() is a useful
         * synchronous snapshot for demos/tests. */
        snap_error_t rc = snap_fetch_once(p->source->url, p->source->auth_header,
                                          &p->parser, p->parser.ctx);
        if (rc != SNAP_OK) return rc;
    }
    return SNAP_OK;
}