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
#include <cjson/cJSON.h>

typedef struct {
    snap_buffer_t buffer;   /* accumulated bytes (NUL-terminated on flush) */
    snap_buffer_t line_buf; /* NDJSON line assembly */
    int is_ndjson;          /* 0 = JSON array/object, 1 = NDJSON lines */
    void (*obj_cb)(cJSON *obj, void *udata);
    void *udata;
} json_parser_ctx_t;

static snap_error_t json_init_impl(void **ctx, const char *config, int ndjson) {
    (void)config;
    if (!ctx) return SNAP_ERR_CONFIG;
    json_parser_ctx_t *c = calloc(1, sizeof(json_parser_ctx_t));
    if (!c) return SNAP_ERR_NOMEM;
    c->is_ndjson = ndjson;
    snap_buffer_init(&c->buffer, 8192);
    snap_buffer_init(&c->line_buf, 4096);
    if (!c->buffer.data || !c->line_buf.data) {
        snap_buffer_free(&c->buffer);
        snap_buffer_free(&c->line_buf);
        free(c);
        return SNAP_ERR_NOMEM;
    }
    *ctx = c;
    return SNAP_OK;
}

static snap_error_t json_init(void **ctx, const char *config) {
    return json_init_impl(ctx, config, 0);
}

static snap_error_t ndjson_init(void **ctx, const char *config) {
    return json_init_impl(ctx, config, 1);
}

void snap_json_set_callback(void *parser_ctx,
                            void (*cb)(struct cJSON *obj, void *udata), void *udata) {
    json_parser_ctx_t *c = parser_ctx;
    if (!c) return;
    c->obj_cb = cb;
    c->udata = udata;
}

static void emit_cjson(json_parser_ctx_t *c, cJSON *obj) {
    if (c->obj_cb && obj) c->obj_cb(obj, c->udata);
}

static snap_error_t json_feed(void *ctx, const snap_chunk_t *chunk) {
    json_parser_ctx_t *c = ctx;
    if (!c || !chunk || !chunk->data) return SNAP_ERR_CONFIG;
    if (chunk->len == 0) return SNAP_OK;

    if (c->is_ndjson) {
        for (size_t i = 0; i < chunk->len; i++) {
            char ch = (char)chunk->data[i];
            if (ch == '\n') {
                if (c->line_buf.len > 0) {
                    snap_buffer_append(&c->line_buf, (uint8_t*)"\0", 1);
                    cJSON *obj = cJSON_Parse((char*)c->line_buf.data);
                    if (obj) {
                        emit_cjson(c, obj);
                        cJSON_Delete(obj);
                    }
                    snap_buffer_reset(&c->line_buf);
                }
            } else if (ch == '\r') {
                continue;
            } else {
                snap_buffer_append(&c->line_buf, &chunk->data[i], 1);
            }
        }
        return SNAP_OK;
    }

    /* Array mode: buffer everything; parse incrementally only when a
     * complete top-level value is available is fragile with cJSON, so
     * accumulate here and parse whole documents on flush. Chunk splits
     * are therefore always safe. */
    snap_buffer_append(&c->buffer, chunk->data, chunk->len);
    return SNAP_OK;
}

static snap_error_t json_flush(void *ctx) {
    json_parser_ctx_t *c = ctx;
    if (!c) return SNAP_ERR_CONFIG;

    if (c->is_ndjson) {
        if (c->line_buf.len > 0) {
            snap_buffer_append(&c->line_buf, (uint8_t*)"\0", 1);
            cJSON *obj = cJSON_Parse((char*)c->line_buf.data);
            if (obj) {
                emit_cjson(c, obj);
                cJSON_Delete(obj);
            }
            snap_buffer_reset(&c->line_buf);
        }
        return SNAP_OK;
    }

    if (c->buffer.len == 0) return SNAP_OK;
    snap_buffer_append(&c->buffer, (uint8_t*)"\0", 1);
    char *text = (char*)c->buffer.data;

    /* Skip leading whitespace. */
    while (*text == ' ' || *text == '\t' || *text == '\n' || *text == '\r') text++;
    if (*text == '\0') { snap_buffer_reset(&c->buffer); return SNAP_OK; }

    cJSON *root = cJSON_Parse(text);
    if (!root) { snap_buffer_reset(&c->buffer); return SNAP_ERR_PARSE; }

    if (cJSON_IsArray(root)) {
        cJSON *item = NULL;
        cJSON_ArrayForEach(item, root) {
            emit_cjson(c, item);
        }
    } else {
        /* Single object (e.g. GBIF search page): emit as one record. */
        emit_cjson(c, root);
    }
    cJSON_Delete(root);
    snap_buffer_reset(&c->buffer);
    return SNAP_OK;
}

static void json_free(void *ctx) {
    json_parser_ctx_t *c = ctx;
    if (!c) return;
    snap_buffer_free(&c->buffer);
    snap_buffer_free(&c->line_buf);
    free(c);
}

/* Clear buffered bytes for a fresh response, keeping obj_cb/udata (the caller
 * registers those once). Without this a re-poll would append the new
 * document onto the previous one and fail to parse. */
static snap_error_t json_reset(void *ctx) {
    json_parser_ctx_t *c = ctx;
    if (!c) return SNAP_ERR_CONFIG;
    snap_buffer_reset(&c->buffer);
    snap_buffer_reset(&c->line_buf);
    return SNAP_OK;
}

const snap_parser_t snap_json_parser = {
    .init = json_init,
    .feed = json_feed,
    .flush = json_flush,
    .free = json_free,
    .reset = json_reset
};

const snap_parser_t snap_ndjson_parser = {
    .init = ndjson_init,
    .feed = json_feed,
    .flush = json_flush,
    .free = json_free,
    .reset = json_reset
};
