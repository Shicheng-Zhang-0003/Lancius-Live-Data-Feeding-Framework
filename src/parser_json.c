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
    snap_buffer_t buffer;
    int in_array;
    int depth;
    void (*obj_cb)(cJSON *obj, void *udata);
    void *udata;
} json_parser_ctx_t;

static snap_error_t json_init(void **ctx, const char *config) {
    json_parser_ctx_t *c = calloc(1, sizeof(json_parser_ctx_t));
    snap_buffer_init(&c->buffer, 8192);
    *ctx = c;
    return SNAP_OK;
}

static snap_error_t json_feed(void *ctx, const snap_chunk_t *chunk) {
    json_parser_ctx_t *c = ctx;
    snap_buffer_append(&c->buffer, chunk->data, chunk->len);
    
    char *start = (char*)c->buffer.data;
    char *end = start + c->buffer.len;
    char *p = start;
    
    while (p < end) {
        if (*p == '[' && !c->in_array) {
            c->in_array = 1;
            c->depth = 1;
            p++;
        } else if (c->in_array) {
            if (*p == '[') c->depth++;
            else if (*p == ']') c->depth--;
            else if (*p == '{') c->depth++;
            else if (*p == '}') c->depth--;
            
            if (c->depth == 1 && (*p == ',' || *p == ']')) {
                *p = '\0';
                cJSON *obj = cJSON_Parse(start);
                if (obj && c->obj_cb) c->obj_cb(obj, c->udata);
                if (obj) cJSON_Delete(obj);
                
                if (*p == ']') c->in_array = 0;
                start = p + 1;
            }
            p++;
        } else {
            p++;
        }
    }
    
    size_t remaining = end - start;
    if (remaining > 0 && start != c->buffer.data) {
        memmove(c->buffer.data, start, remaining);
    }
    c->buffer.len = remaining;
    
    return SNAP_OK;
}

static snap_error_t json_flush(void *ctx) { return SNAP_OK; }

static void json_free(void *ctx) {
    json_parser_ctx_t *c = ctx;
    snap_buffer_free(&c->buffer);
    free(c);
}

const snap_parser_t snap_json_parser = {
    .init = json_init,
    .feed = json_feed,
    .flush = json_flush,
    .free = json_free
};

const snap_parser_t snap_ndjson_parser = {
    .init = json_init,
    .feed = json_feed,
    .flush = json_flush,
    .free = json_free
};