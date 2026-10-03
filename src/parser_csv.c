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
#include "parser_csv.h"
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
    int skip_lines;   /* preamble lines to drop before header (e.g. Daymet "skip=6") */
    int lines_seen;
    void (*row_cb)(const char **fields, int n_fields, void *udata);
    void *udata;
} csv_parser_ctx_t;

static void csv_free_fields(char **fields, int n) {
    if (!fields) return;
    for (int i = 0; i < n; i++) free(fields[i]);
    free(fields);
}

static char **csv_parse_line(const char *line, int *n_fields, char delim) {
    int cap = 16, n = 0;
    char **fields = malloc((size_t)cap * sizeof(char*));
    if (!fields) { *n_fields = 0; return NULL; }
    const char *p = line;

    while (*p) {
        if (n >= cap) {
            cap *= 2;
            char **nf = realloc(fields, (size_t)cap * sizeof(char*));
            if (!nf) { csv_free_fields(fields, n); *n_fields = 0; return NULL; }
            fields = nf;
        }

        if (*p == '"') {
            p++;
            const char *start = p;
            size_t raw = 0;
            while (*p) {
                if (*p == '"' && *(p+1) == '"') {
                    p += 2; raw += 2;
                } else if (*p == '"') {
                    p++; break;
                } else {
                    p++; raw++;
                }
            }
            /* Decoded output is <= raw length ("" -> "). */
            fields[n] = malloc(raw + 1);
            if (!fields[n]) { csv_free_fields(fields, n); *n_fields = 0; return NULL; }
            char *dst = fields[n];
            const char *end = p - 1; /* closing quote (or NUL on unterminated) */
            for (const char *s = start; s < end; s++) {
                if (*s == '"' && *(s+1) == '"') { *dst++ = '"'; s++; }
                else *dst++ = *s;
            }
            *dst = '\0';
        } else {
            const char *start = p;
            while (*p && *p != delim) p++;
            size_t len = (size_t)(p - start);
            fields[n] = malloc(len + 1);
            if (!fields[n]) { csv_free_fields(fields, n); *n_fields = 0; return NULL; }
            memcpy(fields[n], start, len);
            fields[n][len] = '\0';
        }
        n++;
        if (*p == delim) p++;
        /* trailing delimiter implies an empty final field */
        if (*(p-1) == delim && *p == '\0') {
            if (n >= cap) {
                cap *= 2;
                char **nf = realloc(fields, (size_t)cap * sizeof(char*));
                if (!nf) { csv_free_fields(fields, n); *n_fields = 0; return NULL; }
                fields = nf;
            }
            fields[n] = strdup("");
            if (!fields[n]) { csv_free_fields(fields, n); *n_fields = 0; return NULL; }
            n++;
        }
    }

    *n_fields = n;
    return fields;
}

/* config examples: "delimiter=,", "delimiter=;", "skip=6", "skip=6;delimiter=," */
static void csv_parse_config(csv_parser_ctx_t *c, const char *config) {
    c->delimiter = ',';
    c->skip_lines = 0;
    if (!config) return;
    const char *d = strstr(config, "delimiter=");
    if (d && d[10] != '\0') c->delimiter = d[10];
    else if (strchr(config, ';') && !strstr(config, "delimiter=")) c->delimiter = ';';
    const char *s = strstr(config, "skip=");
    if (s) c->skip_lines = atoi(s + 5);
}

static snap_error_t csv_init(void **ctx, const char *config) {
    if (!ctx) return SNAP_ERR_CONFIG;
    csv_parser_ctx_t *c = calloc(1, sizeof(csv_parser_ctx_t));
    if (!c) return SNAP_ERR_NOMEM;
    csv_parse_config(c, config);
    snap_buffer_init(&c->line_buf, 4096);
    snap_buffer_init(&c->out_buf, 8192);
    if (!c->line_buf.data || !c->out_buf.data) {
        snap_buffer_free(&c->line_buf);
        snap_buffer_free(&c->out_buf);
        free(c);
        return SNAP_ERR_NOMEM;
    }
    *ctx = c;
    return SNAP_OK;
}

static void csv_handle_line(csv_parser_ctx_t *c) {
    if (c->line_buf.len == 0) return;
    snap_buffer_append(&c->line_buf, (uint8_t*)"\0", 1);
    char *line = (char*)c->line_buf.data;

    c->lines_seen++;
    if (c->lines_seen <= c->skip_lines) {
        snap_buffer_reset(&c->line_buf);
        return;
    }

    int n_fields = 0;
    char **fields = csv_parse_line(line, &n_fields, c->delimiter);
    if (!fields) { snap_buffer_reset(&c->line_buf); return; }

    if (!c->header_done) {
        /* First non-skipped line is the header: take ownership, do NOT free. */
        c->headers = fields;
        c->n_headers = n_fields;
        c->header_done = 1;
    } else {
        if (c->row_cb && n_fields > 0)
            c->row_cb((const char**)fields, n_fields, c->udata);
        csv_free_fields(fields, n_fields);
    }
    snap_buffer_reset(&c->line_buf);
}

static snap_error_t csv_feed(void *ctx, const snap_chunk_t *chunk) {
    csv_parser_ctx_t *c = ctx;
    if (!c || !chunk || !chunk->data) return SNAP_ERR_CONFIG;

    for (size_t i = 0; i < chunk->len; i++) {
        uint8_t ch = chunk->data[i];
        if (ch == '\n') {
            csv_handle_line(c);
        } else if (ch == '\r') {
            continue; /* tolerate CRLF */
        } else {
            snap_buffer_append(&c->line_buf, &ch, 1);
        }
    }

    return SNAP_OK;
}

static snap_error_t csv_flush(void *ctx) {
    csv_parser_ctx_t *c = ctx;
    if (!c) return SNAP_ERR_CONFIG;
    /* Parse any trailing line without a newline directly (no self-feed). */
    csv_handle_line(c);
    return SNAP_OK;
}

static void csv_free(void *ctx) {
    csv_parser_ctx_t *c = ctx;
    if (!c) return;
    if (c->headers) csv_free_fields(c->headers, c->n_headers);
    snap_buffer_free(&c->line_buf);
    snap_buffer_free(&c->out_buf);
    free(c);
}

void snap_csv_set_callback(void *parser_ctx, csv_row_cb_t cb, void *udata) {
    csv_parser_ctx_t *c = parser_ctx;
    if (!c) return;
    c->row_cb = cb;
    c->udata = udata;
}

const snap_parser_t snap_csv_parser = {
    .init = csv_init,
    .feed = csv_feed,
    .flush = csv_flush,
    .free = csv_free
};
