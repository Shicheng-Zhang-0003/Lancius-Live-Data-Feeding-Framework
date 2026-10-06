/*
 * LDFD - Output sinks
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

/*
 * The framework's output stage. Before this existed the pipeline was
 * source -> parser with no way to land a byte anywhere: every
 * snap_output_t in the header was an undefined extern.
 *
 * Two sinks are real:
 *   snap_file_output    - append raw bytes to a file (the integration path:
 *                        this is how fetched rows reach the Lancius trainer)
 *   snap_callback_output - hand each chunk to a registered C function
 */

#include "snapshot.h"
#include "output.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ *
 * File sink
 * ------------------------------------------------------------------ */

typedef struct {
    FILE *fp;
    char *path;
    int   owns_fp;      /* close on free? 0 when wrapping an existing FILE* */
    long  bytes;        /* bytes written, for reporting */
    long  chunks;
} file_out_ctx_t;

static snap_error_t file_out_init(void **ctx, const char *config) {
    if (!ctx || !config || !*config) return SNAP_ERR_CONFIG;
    file_out_ctx_t *c = calloc(1, sizeof *c);
    if (!c) return SNAP_ERR_NOMEM;

    c->path = strdup(config);
    if (!c->path) { free(c); return SNAP_ERR_NOMEM; }

    /* "w" truncates, "a" appends. Default "a": a re-polled source must not
     * silently erase what the previous poll delivered. */
    const char *mode = "a";
    size_t n = strlen(c->path);
    /* ":w" is a mode marker: strip it and truncate. Checked before the
     * append so a re-run can reset a previous fetch rather than stacking
     * duplicates. */
    if (n > 2 && c->path[n - 2] == ':' &&
        (c->path[n - 1] == 'w' || c->path[n - 1] == 'W')) {
        mode = "w";
        c->path[n - 2] = '\0';
    }

    c->fp = fopen(c->path, mode);
    if (!c->fp) {
        free(c->path);
        free(c);
        return SNAP_ERR_OUTPUT;
    }
    c->owns_fp = 1;
    *ctx = c;
    return SNAP_OK;
}

static snap_error_t file_out_write(void *ctx, const snap_chunk_t *chunk) {
    file_out_ctx_t *c = ctx;
    if (!c || !chunk) return SNAP_ERR_CONFIG;
    if (!chunk->data || chunk->len == 0) return SNAP_OK;
    size_t w = fwrite(chunk->data, 1, chunk->len, c->fp);
    if (w != chunk->len) return SNAP_ERR_OUTPUT;   /* disk full, etc. */
    c->bytes += (long)chunk->len;
    c->chunks++;
    return SNAP_OK;
}

static snap_error_t file_out_close(void *ctx) {
    file_out_ctx_t *c = ctx;
    if (!c || !c->fp) return SNAP_OK;
    /* Despot truth: fflush failure (ENOSPC) was invisible. Surface it. */
    if (fflush(c->fp) != 0) return SNAP_ERR_OUTPUT;
    return SNAP_OK;
}

static void file_out_free(void *ctx) {
    file_out_ctx_t *c = ctx;
    if (!c) return;
    if (c->fp && c->owns_fp) fclose(c->fp);
    free(c->path);
    free(c);
}

/* Attach an already-open stream (used by the Python bridge so it can hand
 * back a file object it owns). config is unused. */
snap_error_t snap_file_output_attach(void **ctx, void *fp) {
    if (!ctx || !fp) return SNAP_ERR_CONFIG;
    file_out_ctx_t *c = calloc(1, sizeof *c);
    if (!c) return SNAP_ERR_NOMEM;
    c->fp = fp;
    c->owns_fp = 0;
    *ctx = c;
    return SNAP_OK;
}

const snap_output_t snap_file_output = {
    .init = file_out_init,
    .write = file_out_write,
    .close = file_out_close,
    .free = file_out_free
};

/* ------------------------------------------------------------------ *
 * Callback sink
 * ------------------------------------------------------------------ */

typedef struct {
    snap_chunk_cb_t cb;
    void *udata;
} cb_out_ctx_t;

static snap_error_t cb_out_init(void **ctx, const char *config) {
    (void)config;
    if (!ctx) return SNAP_ERR_CONFIG;
    cb_out_ctx_t *c = calloc(1, sizeof *c);
    if (!c) return SNAP_ERR_NOMEM;
    *ctx = c;
    return SNAP_OK;
}

static snap_error_t cb_out_write(void *ctx, const snap_chunk_t *chunk) {
    cb_out_ctx_t *c = ctx;
    if (!c || !chunk) return SNAP_ERR_CONFIG;
    if (!c->cb) return SNAP_OK;
    c->cb(chunk, c->udata);
    return SNAP_OK;
}

static snap_error_t cb_out_close(void *ctx) { (void)ctx; return SNAP_OK; }

static void cb_out_free(void *ctx) { free(ctx); }

const snap_output_t snap_callback_output = {
    .init = cb_out_init,
    .write = cb_out_write,
    .close = cb_out_close,
    .free = cb_out_free
};

snap_error_t snap_callback_set(void *output_ctx, snap_chunk_cb_t cb, void *udata) {
    cb_out_ctx_t *c = output_ctx;
    if (!c) return SNAP_ERR_CONFIG;
    c->cb = cb;
    c->udata = udata;
    return SNAP_OK;
}

/* ------------------------------------------------------------------ *
 * Row sink: CSV rows -> newline-delimited JSON objects
 * ------------------------------------------------------------------ */

/*
 * This is the bridge into the Lancius training pipeline: the Python side
 * registers one of these per CSV source and writes .jsonl straight into
 * data_text/, which is exactly what distill_prm800k and the tokenizer
 * manifests already consume. Field names come from the CSV header.
 */

typedef struct {
    FILE *fp;
    char *path;
    char **names;
    int    n_names;
    int    header_done;
    long   rows;
} row_out_ctx_t;

static void row_out_free_names(row_out_ctx_t *r) {
    if (!r->names) return;
    for (int i = 0; i < r->n_names; i++) free(r->names[i]);
    free(r->names);
    r->names = NULL;
    r->n_names = 0;
}

static void json_escape(const char *in, char *out, size_t cap) {
    if (!out || cap == 0) return;
    out[0] = '\0';
    if (!in) return;
    size_t j = 0;
    for (size_t i = 0; in[i] && j + 7 < cap; i++) {
        unsigned char c = (unsigned char)in[i];
        switch (c) {
            case '"':  out[j++] = '\\'; out[j++] = '"';  break;
            case '\\': out[j++] = '\\'; out[j++] = '\\'; break;
            case '\n': out[j++] = '\\'; out[j++] = 'n';  break;
            case '\r': out[j++] = '\\'; out[j++] = 'r';  break;
            case '\t': out[j++] = '\\'; out[j++] = 't';  break;
            default:
                if (c < 0x20) {
                    j += (size_t)snprintf(out + j, cap - j, "\\u%04x", c);
                } else {
                    out[j++] = (char)c;
                }
        }
    }
    out[j] = '\0';
}

/* Write a value into a stream as JSON: bare number when it parses cleanly
 * (so training code gets real numbers, not strings), else a JSON string.
 *
 * strtod maps overflow to +/-HUGE_VAL and unparseable junk to 0, so a finite
 * check is required: "%g" would otherwise emit `inf` / `nan`, which is not
 * valid JSON and would poison every downstream consumer. Non-finite input
 * becomes the string "Infinity"/"NaN" so the record survives as data. */
static void emit_value_fp(FILE *fp, const char *v) {
    if (!v || !*v) { fputs("null", fp); return; }
    char *end = NULL;
    errno = 0;
    double d = strtod(v, &end);
    int numeric = (end && end != v && *end == '\0' && errno != ERANGE);
    if (numeric && d == d && d != HUGE_VAL && d != -HUGE_VAL) {
        /* Shortest representation that round-trips. A bare "%.17g" is exact
         * but turns -113 into -113.00000000000000 and 49.9 into
         * 49.900000000000006, which is noisy for a dataset and needlessly
         * perturbs values. Walking the precision up stops at the first
         * form that reads back identically. */
        char num[64];
        for (int prec = 1; prec <= 17; prec++) {
            snprintf(num, sizeof num, "%.*g", prec, d);
            if (strtod(num, NULL) == d) break;
        }
        fputs(num, fp);
        return;
    }
    if (numeric && (d == HUGE_VAL || d == -HUGE_VAL)) {
        /* Overflowed the double range: keep it as text, do not emit `inf`. */
        char esc[256];
        json_escape(v, esc, sizeof esc);
        fprintf(fp, "\"%s\"", esc);
        return;
    }
    if (d != d) {                      /* NaN literal in the input */
        fputs("\"NaN\"", fp);
        return;
    }
    char esc[1024];
    json_escape(v, esc, sizeof esc);
    fprintf(fp, "\"%s\"", esc);
}

snap_error_t snap_row_out_init(void **ctx, const char *config) {
    if (!ctx || !config || !*config) return SNAP_ERR_CONFIG;
    row_out_ctx_t *r = calloc(1, sizeof *r);
    if (!r) return SNAP_ERR_NOMEM;
    r->path = strdup(config);
    if (!r->path) { free(r); return SNAP_ERR_NOMEM; }
    const char *mode = "a";
    size_t n = strlen(r->path);
    if (n > 2 && r->path[n - 2] == ':' &&
        (r->path[n - 1] == 'w' || r->path[n - 1] == 'W')) {
        mode = "w";
        r->path[n - 2] = '\0';
    }
    r->fp = fopen(r->path, mode);
    if (!r->fp) { free(r->path); free(r); return SNAP_ERR_OUTPUT; }
    *ctx = r;
    return SNAP_OK;
}

void snap_row_out_set_stream(void *ctx, void *fp) {
    row_out_ctx_t *r = ctx;
    if (!r || !fp) return;
    if (r->fp && r->fp != fp) fclose(r->fp);
    r->fp = fp;
}

void snap_row_out_set_header(void *ctx, char **names, int n_names) {
    row_out_ctx_t *r = ctx;
    if (!r) return;
    row_out_free_names(r);
    if (!names || n_names <= 0) return;
    r->names = calloc((size_t)n_names, sizeof(char *));
    if (!r->names) return;
    for (int i = 0; i < n_names; i++) {
        r->names[i] = strdup(names[i] ? names[i] : "");
        if (!r->names[i]) { r->n_names = i; return; }
    }
    r->n_names = n_names;
}

/* Write one row as a JSON object. Extra fields beyond the header are
 * dropped and missing fields become null; neither is silently misaligned. */
void snap_row_out_emit(void *ctx, const char **fields, int n_fields) {
    row_out_ctx_t *r = ctx;
    if (!r || !r->fp) return;
    fputc('{', r->fp);
    for (int i = 0; i < n_fields; i++) {
        if (i) fputc(',', r->fp);
        char esc[256];
        const char *name = (i < r->n_names && r->names) ? r->names[i] : NULL;
        if (!name || !*name) {
            char tmp[32];
            snprintf(tmp, sizeof tmp, "field_%d", i);
            json_escape(tmp, esc, sizeof esc);
        } else {
            json_escape(name, esc, sizeof esc);
        }
        fprintf(r->fp, "\"%s\":", esc);
        emit_value_fp(r->fp, fields[i]);
    }
    fputs("}\n", r->fp);
    r->rows++;
}

snap_error_t snap_row_out_close(void *ctx) {
    row_out_ctx_t *r = ctx;
    if (!r || !r->fp) return SNAP_OK;
    return fflush(r->fp) == 0 ? SNAP_OK : SNAP_ERR_OUTPUT;
}

void snap_row_out_free(void *ctx) {
    row_out_ctx_t *r = ctx;
    if (!r) return;
    if (r->fp) fclose(r->fp);
    row_out_free_names(r);
    free(r->path);
    free(r);
}