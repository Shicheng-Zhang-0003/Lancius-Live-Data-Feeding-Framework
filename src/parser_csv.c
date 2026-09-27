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
#include <ctype.h>

typedef struct {
    snap_buffer_t line_buf;
    snap_buffer_t out_buf;
    int header_done;
    char **headers;
    int n_headers;
    char delimiter;
    void (*row_cb)(const char **fields, int n_fields, void *udata);
    void *udata;
} csv_parser_ctx_t;

static void csv_free_fields(char **fields, int n) {
    for (int i = 0; i < n; i++) free(fields[i]);
    free(fields);
}

static char **csv_parse_line(const char *line, int *n_fields, char delim) {
    int cap = 16, n = 0;
    char **fields = malloc(cap * sizeof(char*));
    const char *p = line;
    
    while (*p) {
        if (n >= cap) {
            cap *= 2;
            fields = realloc(fields, cap * sizeof(char*));
        }
        
        if (*p == '"') {
            p++;
            const char *start = p;
            size_t len = 0;
            while (*p) {
                if (*p == '"' && *(p+1) == '"') {
                    p += 2; len += 2;
                } else if (*p == '"') {
                    p++; break;
                } else {
                    p++; len++;
                }
            }
            fields[n] = malloc(len + 1);
            char *dst = fields[n];
            for (const char *s = start; s < p - 1; s++) {
                if (*s == '"' && *(s+1) == '"') { *dst++ = '"'; s++; }
                else *dst++ = *s;
            }
            *dst = '\0';
        } else {
            const char *start = p;
            while (*p && *p != delim) p++;
            size_t len = p - start;
            fields[n] = malloc(len + 1);
            memcpy(fields[n], start, len);
            fields[n][len] = '\0';
        }
        n++;
        if (*p == delim) p++;
    }
    
    *n_fields = n;
    return fields;
}

static snap_error_t csv_init(void **ctx, const char *config) {
    csv_parser_ctx_t *c = calloc(1, sizeof(csv_parser_ctx_t));
    c->delimiter = config && strchr(config, ';') ? ';' : ',';
    snap_buffer_init(&c->line_buf, 4096);
    snap_buffer_init(&c->out_buf, 8192);
    *ctx = c;
    return SNAP_OK;
}

static snap_error_t csv_feed(void *ctx, const snap_chunk_t *chunk) {
    csv_parser_ctx_t *c = ctx;
    
    for (size_t i = 0; i < chunk->len; i++) {
        uint8_t ch = chunk->data[i];
        
        if (ch == '\n' || ch == '\r') {
            if (c->line_buf.len > 0) {
                snap_buffer_append(&c->line_buf, (uint8_t*)"\0", 1);
                char *line = (char*)c->line_buf.data;
                
                int n_fields;
                char **fields = csv_parse_line(line, &n_fields, c->delimiter);
                
                if (!c->header_done) {
                    c->headers = fields;
                    c->n_headers = n_fields;
                    c->header_done = 1;
                } else if (c->row_cb) {
                    c->row_cb((const char**)fields, n_fields, c->udata);
                }
                
                if (c->header_done || !c->row_cb) {
                    csv_free_fields(fields, n_fields);
                }
                
                snap_buffer_reset(&c->line_buf);
            }
        } else {
            snap_buffer_append(&c->line_buf, &ch, 1);
        }
    }
    
    return SNAP_OK;
}

static snap_error_t csv_flush(void *ctx) {
    csv_parser_ctx_t *c = ctx;
    if (c->line_buf.len > 0) {
        snap_chunk_t final = { c->line_buf.data, c->line_buf.len, 1 };
        csv_feed(ctx, &final);
    }
    return SNAP_OK;
}

static void csv_free(void *ctx) {
    csv_parser_ctx_t *c = ctx;
    if (c->headers) csv_free_fields(c->headers, c->n_headers);
    snap_buffer_free(&c->line_buf);
    snap_buffer_free(&c->out_buf);
    free(c);
}

void snap_csv_set_callback(void *parser_ctx, csv_row_cb_t cb, void *udata) {
    csv_parser_ctx_t *c = parser_ctx;
    c->row_cb = cb;
    c->udata = udata;
}

const snap_parser_t snap_csv_parser = {
    .init = csv_init,
    .feed = csv_feed,
    .flush = csv_flush,
    .free = csv_free
};