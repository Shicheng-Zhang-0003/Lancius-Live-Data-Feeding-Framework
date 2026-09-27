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

snap_ctx_t *snap_ctx_new(void) {
    snap_ctx_t *ctx = calloc(1, sizeof(snap_ctx_t));
    curl_global_init(CURL_GLOBAL_DEFAULT);
    return ctx;
}

void snap_ctx_free(snap_ctx_t *ctx) {
    if (!ctx) return;
    snap_ctx_stop(ctx);
    
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
    snap_pipeline_t *p = realloc(ctx->pipelines, (ctx->n_pipelines + 1) * sizeof(snap_pipeline_t));
    if (!p) return SNAP_ERR_NOMEM;
    ctx->pipelines = p;
    
    snap_pipeline_t *pipe = &ctx->pipelines[ctx->n_pipelines];
    memset(pipe, 0, sizeof(snap_pipeline_t));
    
    pipe->source = malloc(sizeof(snap_source_t));
    memcpy(pipe->source, src, sizeof(snap_source_t));
    pipe->source->name = strdup(src->name);
    pipe->source->url = strdup(src->url);
    pipe->source->auth_header = src->auth_header ? strdup(src->auth_header) : NULL;
    pipe->source->parser_config = src->parser_config ? strdup(src->parser_config) : NULL;
    pipe->source->transform_config = src->transform_config ? strdup(src->transform_config) : NULL;
    pipe->source->output_config = src->output_config ? strdup(src->output_config) : NULL;
    pipe->source->next = NULL;
    
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
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        if (p->source->interval_sec == 0) {
            snap_fetch_once(p->source->url, p->source->auth_header, &p->parser, p->parser.ctx);
        }
    }
    return SNAP_OK;
}