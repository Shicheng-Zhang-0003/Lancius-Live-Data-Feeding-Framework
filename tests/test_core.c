/*
 * LDFD - Core framework tests (parsers, buffer, outputs)
 * Copyright (C) 2026
 *
 * Build:
 *   gcc -std=c17 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude \
 *       tests/test_core.c src/buffer.c src/parser_csv.c src/parser_json.c \
 *       src/output.c -o /tmp/test_core && /tmp/test_core
 *
 * Network-free and dependency-free: no libcurl, no cJSON. Only the parts of
 * the framework that do not speak HTTP are covered here (buffer, CSV parser,
 * outputs). Context lifecycle, the fetch loop and the interval scheduler
 * need libcurl and live in tests/test_fetch.c.
 */

#include "snapshot.h"
#include "output.h"
#include "parser_csv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef TMPDIR_TMPL
#define TMPDIR_TMPL "/tmp/ldfd_core_XXXXXX"
#endif

static int failures;
static int checks;

static void ok(int cond, const char *what) {
    checks++;
    if (cond) {
        printf("PASS %s\n", what);
    } else {
        printf("FAIL %s\n", what);
        failures++;
    }
}

/* ---------------- buffer ---------------- */

static void test_buffer(void) {
    snap_buffer_t b;
    snap_buffer_init(&b, 4);
    ok(b.data != NULL && b.cap >= 4, "buffer: init allocates");

    /* grow past the initial capacity */
    for (int i = 0; i < 100; i++) snap_buffer_append(&b, (const uint8_t *)"x", 1);
    ok(b.len == 100, "buffer: append grows and tracks length");

    snap_buffer_reset(&b);
    ok(b.len == 0 && b.data != NULL, "buffer: reset keeps allocation");

    /* append after reset must reuse, not leak */
    snap_buffer_append(&b, (const uint8_t *)"ok", 2);
    ok(b.len == 2 && memcmp(b.data, "ok", 2) == 0, "buffer: usable after reset");

    snap_buffer_append(&b, NULL, 5);
    ok(b.len == 2, "buffer: NULL data is a no-op, not a crash");
    snap_buffer_append(NULL, (const uint8_t *)"x", 1);
    ok(1, "buffer: NULL buffer is a no-op");

    /* Growth arithmetic must not wrap. A length near SIZE_MAX used to skip
     * the grow entirely (then memcpy past the end) or spin in the doubling
     * loop once `new_cap *= 2` wrapped to 0. */
    {
        snap_buffer_t big;
        snap_buffer_init(&big, 4);
        char one[1] = { 0 };
        snap_buffer_append(&big, (const uint8_t *)one, (size_t)1 << 62);
        ok(big.len == 0 && big.cap == 4,
           "buffer: absurd length refused instead of wrapping");
        snap_buffer_free(&big);

        snap_buffer_t big2;
        snap_buffer_init(&big2, 4);
        /* fill past the cap repeatedly to exercise the doubling path */
        char block[64];
        memset(block, 'z', sizeof block);
        for (int i = 0; i < 40; i++)
            snap_buffer_append(&big2, (const uint8_t *)block, sizeof block);
        ok(big2.len == 40 * (int)sizeof block, "buffer: repeated growth tracks length");
        int intact = 1;
        for (size_t i = 0; i < big2.len; i++) if (big2.data[i] != 'z') { intact = 0; break; }
        ok(intact, "buffer: contents intact after many reallocations");
        snap_buffer_free(&big2);
    }

    snap_buffer_free(&b);
    ok(b.data == NULL && b.cap == 0, "buffer: free clears");
    snap_buffer_free(&b);
    ok(1, "buffer: double free is safe");
}

/* ---------------- CSV parser ---------------- */

static int csv_rows;
static char csv_first[128];
static void on_row(const char **f, int n, void *u) {
    (void)u;
    csv_rows++;
    snprintf(csv_first, sizeof csv_first, "%s", n > 0 ? f[0] : "");
}

/* Drive the CSV parser directly through its vtable. snap_ctx_* lives in
 * context.c which talks to libcurl, so the libc-only suite uses the parser
 * on its own -- which is the same code path a fetch feeds. */
typedef struct {
    snap_parser_t parser;
} csv_harness_t;

static int csv_harness_init(csv_harness_t *h, char *parser_config) {
    memset(h, 0, sizeof *h);
    h->parser = snap_csv_parser;
    if (h->parser.init(&h->parser.ctx, parser_config) != SNAP_OK) return 0;
    snap_csv_set_callback(h->parser.ctx, on_row, NULL);
    return 1;
}

static void csv_harness_free(csv_harness_t *h) {
    if (h->parser.free && h->parser.ctx) h->parser.free(h->parser.ctx);
    h->parser.ctx = NULL;
}

static void csv_feed_all(csv_harness_t *h, const char *text) {
    size_t len = strlen(text);
    snap_chunk_t c = { (uint8_t *)text, len, 0 };
    h->parser.feed(h->parser.ctx, &c);
    h->parser.flush(h->parser.ctx);
}


static void test_csv_quoting(void) {
    struct { const char *in; int want_rows; const char *want_first; } t[] = {
        { "a,b,c\n1,2,3\n",                     1, "1" },
        { "a,b,c\n\"x\",\"y\",\"z\"\n",         1, "x" },
        { "a,b,c\n\"x\"\"y\",2,3\n",             1, "x\"y" },
        { "a,b,c\n1,2,\n",                       1, "1" },
        { "a,b,c\n\"\",2,3\n",                   1, "" },
        { "a,b,c\n,,\n",                         1, "" },
    };
    for (unsigned i = 0; i < sizeof t / sizeof *t; i++) {
        csv_harness_t h;
        if (!csv_harness_init(&h, NULL)) { ok(0, "csv: harness built"); return; }
        csv_rows = 0; csv_first[0] = '\0';
        csv_feed_all(&h, t[i].in);
        ok(csv_rows == t[i].want_rows, "csv: row count matches expectation");
        if (t[i].want_first[0]) {
            ok(strcmp(csv_first, t[i].want_first) == 0,
               "csv: first field value matches expectation");
        }
        csv_harness_free(&h);
    }
}

static void test_csv_crlf_and_skip(void) {
    static char skip_cfg[] = "skip=2";
    csv_harness_t h;
    if (!csv_harness_init(&h, NULL)) { ok(0, "crlf: harness built"); return; }
    csv_rows = 0;
    csv_feed_all(&h, "a,b\r\n1,2\r\n3,4\r\n");
    ok(csv_rows == 2, "csv: CRLF tolerated, 2 data rows");
    csv_harness_free(&h);

    if (!csv_harness_init(&h, skip_cfg)) { ok(0, "skip: harness built"); return; }
    csv_rows = 0;
    csv_feed_all(&h, "preamble one\npreamble two\na,b\n1,2\n");
    ok(csv_rows == 1, "csv: skip=N drops N preamble lines");
    csv_harness_free(&h);
}

/* The header-replay defect: without parser->reset a re-poll emitted the
 * header row as data, so a 1s-interval source produced a bogus leading
 * record on every poll after the first. */
static void test_csv_no_header_replay(void) {
    csv_harness_t h;
    if (!csv_harness_init(&h, NULL)) { ok(0, "replay: harness built"); return; }

    csv_rows = 0;
    csv_feed_all(&h, "latitude,longitude\n32.0,-81.0\n");
    ok(csv_rows == 1, "replay: first poll emits 1 data row");

    /* what snap_pipeline_rearm() does between polls */
    ok(h.parser.reset != NULL, "replay: CSV parser provides reset()");
    h.parser.reset(h.parser.ctx);
    csv_rows = 0; csv_first[0] = '\0';
    csv_feed_all(&h, "latitude,longitude\n33.0,-82.0\n");
    ok(csv_rows == 1, "replay: second poll emits 1 data row (not 2)");
    ok(strcmp(csv_first, "33.0") == 0,
       "replay: header row is not re-emitted as data");

    csv_harness_free(&h);
}

static void test_csv_split_chunks(void) {
    csv_harness_t h;
    if (!csv_harness_init(&h, NULL)) { ok(0, "split: harness built"); return; }
    static const char *body = "a,b,c\n1,2,3\n4,5,6\n";
    csv_rows = 0;
    /* one byte at a time: worst-case chunking */
    for (size_t i = 0; i < strlen(body); i++) {
        snap_chunk_t c = { (uint8_t *)(body + i), 1, 0 };
        h.parser.feed(h.parser.ctx, &c);
    }
    h.parser.flush(h.parser.ctx);
    ok(csv_rows == 2, "csv: byte-at-a-time chunking yields both rows");
    csv_harness_free(&h);
}

/* ---------------- outputs ---------------- */

static int cb_hits;
static long cb_bytes;
static void on_chunk(const snap_chunk_t *ch, void *u) {
    (void)u;
    cb_hits++;
    cb_bytes += (long)ch->len;
}

static void test_outputs(void) {
    char tmpl[] = TMPDIR_TMPL;
    int fd = mkstemp(tmpl);
    if (fd >= 0) close(fd);

    /* file sink, truncating */
    {
        char path[256];
        snprintf(path, sizeof path, "%s:w", tmpl);
        void *ctx = NULL;
        ok(snap_file_output.init(&ctx, path) == SNAP_OK, "output: file sink init");
        snap_chunk_t a = { (uint8_t *)"hello ", 6, 0 };
        snap_chunk_t b = { (uint8_t *)"world", 5, 0 };
        ok(snap_file_output.write(ctx, &a) == SNAP_OK, "output: write a");
        ok(snap_file_output.write(ctx, &b) == SNAP_OK, "output: write b");
        ok(snap_file_output.close(ctx) == SNAP_OK, "output: close");
        snap_file_output.free(ctx);

        FILE *f = fopen(tmpl, "rb");
        char buf[64] = { 0 };
        size_t n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
        if (f) fclose(f);
        ok(n == 11 && strcmp(buf, "hello world") == 0,
           "output: file sink wrote all 11 bytes");
    }

    /* default mode must append, so a re-poll cannot erase prior data */
    {
        void *ctx = NULL;
        ok(snap_file_output.init(&ctx, tmpl) == SNAP_OK, "output: append init");
        snap_chunk_t a = { (uint8_t *)"!", 1, 0 };
        snap_file_output.write(ctx, &a);
        snap_file_output.close(ctx);
        snap_file_output.free(ctx);
        FILE *f = fopen(tmpl, "rb");
        char buf[64] = { 0 };
        size_t n = f ? fread(buf, 1, sizeof buf - 1, f) : 0;
        if (f) fclose(f);
        ok(n == 12, "output: append mode preserves earlier content");
    }

    /* callback sink */
    {
        void *ctx = NULL;
        snap_callback_output.init(&ctx, NULL);
        snap_callback_set(ctx, on_chunk, NULL);
        cb_hits = 0; cb_bytes = 0;
        snap_chunk_t a = { (uint8_t *)"abc", 3, 0 };
        snap_callback_output.write(ctx, &a);
        ok(cb_hits == 1 && cb_bytes == 3,
           "output: callback sink fired once with 3 bytes");
        snap_callback_output.free(ctx);
    }

    /* row sink: numbers stay numbers so the trainer gets usable types */
    {
        remove(tmpl);
        void *ctx = NULL;
        ok(snap_row_out_init(&ctx, tmpl) == SNAP_OK, "output: row sink init");
        char *names[] = { "lat", "lon", "frp", "sat" };
        snap_row_out_set_header(ctx, names, 4);
        const char *r1[] = { "49.9", "-113.0", "15.46", "T" };
        const char *r2[] = { "50.1", "-112.2", "7.25", "N" };
        snap_row_out_emit(ctx, r1, 4);
        snap_row_out_emit(ctx, r2, 4);
        snap_row_out_close(ctx);
        snap_row_out_free(ctx);

        FILE *f = fopen(tmpl, "rb");
        char buf[512] = { 0 };
        if (f) { size_t n = fread(buf, 1, sizeof buf - 1, f); (void)n; fclose(f); }
        ok(strstr(buf, "\"lat\":49.9") != NULL, "output: lat emitted as a number");
        ok(strstr(buf, "\"frp\":15.46") != NULL, "output: frp emitted as a number");
        ok(strstr(buf, "\"sat\":\"T\"") != NULL, "output: sat emitted as a string");
    }

    /* ragged rows must not misalign silently */
    {
        remove(tmpl);
        void *ctx = NULL;
        snap_row_out_init(&ctx, tmpl);
        char *names[] = { "a", "b" };
        snap_row_out_set_header(ctx, names, 2);
        const char *short_row[] = { "1" };
        const char *long_row[] = { "1", "2", "3", "4" };
        snap_row_out_emit(ctx, short_row, 1);
        snap_row_out_emit(ctx, long_row, 4);
        snap_row_out_close(ctx);
        snap_row_out_free(ctx);

        FILE *f = fopen(tmpl, "rb");
        char buf[512] = { 0 };
        if (f) { size_t n = fread(buf, 1, sizeof buf - 1, f); (void)n; fclose(f); }
        ok(strstr(buf, "\"a\":1}") != NULL,
           "output: short row emits only the fields present");
        ok(strstr(buf, "field_2") != NULL,
           "output: extra field gets a synthetic name, not a silent shift");
    }

    /* Non-finite / out-of-range values must not produce invalid JSON.
     * strtod overflows to HUGE_VAL and "%g" would print `inf`, which no
     * JSON parser accepts -- so the record has to survive as text instead. */
    {
        remove(tmpl);
        void *ctx = NULL;
        snap_row_out_init(&ctx, tmpl);
        char *names[] = { "a", "b", "c", "d" };
        snap_row_out_set_header(ctx, names, 4);
        const char *row[] = { "1e999", "-1e999", "nan", "inf" };
        snap_row_out_emit(ctx, row, 4);
        snap_row_out_close(ctx);
        snap_row_out_free(ctx);

        FILE *f = fopen(tmpl, "rb");
        char buf[512] = { 0 };
        if (f) { size_t n = fread(buf, 1, sizeof buf - 1, f); (void)n; fclose(f); }
        ok(strstr(buf, "inf") == NULL || strstr(buf, "\"inf\"") != NULL,
           "output: bare `inf` never appears (would be invalid JSON)");
        ok(strstr(buf, "\"1e999\"") != NULL,
           "output: out-of-range number kept as a string");
        ok(strstr(buf, "\"NaN\"") != NULL,
           "output: NaN rendered as the string \"NaN\"");
        ok(strchr(buf, '{') != NULL && strchr(buf, '}') != NULL,
           "output: row is still well-formed JSON");
    }

    /* Large integers must keep full precision rather than being rounded
     * into exponent notation by a short %g. */
    {
        remove(tmpl);
        void *ctx = NULL;
        snap_row_out_init(&ctx, tmpl);
        char *names[] = { "id" };
        snap_row_out_set_header(ctx, names, 1);
        const char *row[] = { "123456789012345" };
        snap_row_out_emit(ctx, row, 1);
        snap_row_out_close(ctx);
        snap_row_out_free(ctx);
        FILE *f = fopen(tmpl, "rb");
        char buf[256] = { 0 };
        if (f) { size_t n = fread(buf, 1, sizeof buf - 1, f); (void)n; fclose(f); }
        ok(strstr(buf, "123456789012345") != NULL,
           "output: 15-digit integer survives without exponent notation");
    }

    /* NULL safety */
    {
        ok(snap_file_output.init(NULL, "x") == SNAP_ERR_CONFIG,
           "output: file init rejects NULL ctx");
        ok(snap_file_output.write(NULL, NULL) == SNAP_ERR_CONFIG,
           "output: file write rejects NULL ctx");
        snap_file_output.free(NULL);
        snap_row_out_free(NULL);
        ok(1, "output: free(NULL) is safe");
    }

    remove(tmpl);
}

int main(void) {
    printf("== core framework suite ==\n");
    test_buffer();
    test_csv_quoting();
    test_csv_crlf_and_skip();
    test_csv_no_header_replay();
    test_csv_split_chunks();
    test_outputs();

    printf("\n%d checks, %d failures\n", checks, failures);
    if (failures) {
        printf("CORE SUITE FAILED\n");
        return 1;
    }
    printf("core: all pass\n");
    return 0;
}