/*
 * LDFD - Regression tests for defects found in the v12R2 integration audit.
 * Build: gcc -std=c17 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude \
 *          tests/test_assoc_regress.c src/assoc.c -o /tmp/test_assoc_regress \
 *          && /tmp/test_assoc_regress
 *
 * Network-free, libc-only. Every case here reproduces a bug that shipped in
 * the "audit hardening complete" commit; each must fail on the old code.
 */
#include "assoc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

static void okf(int cond, const char *fmt, ...) {
    (void)fmt;
    checks++;
    if (!cond) {
        printf("FAIL\n");
        failures++;
    }
}

/* ------------------------------------------------------------------ *
 * 1. NULL callback used to segfault on the first data row.
 *    assoc_fire_feed_new / assoc_climate_feed_new must refuse NULL.
 * ------------------------------------------------------------------ */
static void test_null_callback_rejected(void) {
    assoc_fire_opts_t o = { -1, NULL };
    ok(assoc_fire_feed_new(NULL, NULL, &o) == NULL,
       "fire_feed_new(NULL cb) returns NULL instead of crashing later");
    ok(assoc_climate_feed_new(NULL, NULL) == NULL,
       "climate_feed_new(NULL cb) returns NULL instead of crashing later");
}

/* ------------------------------------------------------------------ *
 * 2. Headerless feeds emitted zero rows.
 *    The readme promises a positional fallback when no header is
 *    present; the old code only ever forwarded rows after seeing a
 *    header, so the fallback was unreachable.
 * ------------------------------------------------------------------ */
static int hdr_fire_n;
static assoc_fire_t hdr_fire;
static void on_hdr_fire(const assoc_fire_t *f, void *u) {
    (void)u;
    hdr_fire_n++;
    hdr_fire = *f;
}

static void test_headerless_positional_fallback(void) {
    /* MODIS C6.1 positional order, no header row at all. */
    static const char row[] =
        "32.07066,-81.8846,302.66,1.71,1.28,2026-09-26,0209,T,51,6.1NRT,286.35,15.46,N\n";
    assoc_fire_opts_t o = { -1, NULL };
    hdr_fire_n = 0;
    memset(&hdr_fire, 0, sizeof hdr_fire);

    assoc_fire_feed_t *f = assoc_fire_feed_new(on_hdr_fire, NULL, &o);
    ok(f != NULL, "headerless: feed constructed");
    if (!f) return;
    assoc_fire_feed(f, (const unsigned char *)row, sizeof row - 1);
    assoc_fire_feed_free(f);

    ok(hdr_fire_n == 1, "headerless: exactly one row emitted (was 0)");
    ok(hdr_fire.lat > 32.07 && hdr_fire.lat < 32.08, "headerless: lat by position");
    ok(hdr_fire.frp > 15.4 && hdr_fire.frp < 15.5, "headerless: frp by position");
    ok(strcmp(hdr_fire.acq_date, "2026-09-26") == 0, "headerless: acq_date by position");
    ok(strcmp(hdr_fire.satellite, "T") == 0, "headerless: satellite by position");
}

/* ------------------------------------------------------------------ *
 * 3. Daymet preamble must still be skipped when no header ever
 *    arrives (prose lines start with a letter, data rows with a digit).
 * ------------------------------------------------------------------ */
static int pre_clim_n;
static void on_pre_clim(const assoc_climate_t *c, void *u) {
    (void)u;
    (void)c;
    pre_clim_n++;
}

static void test_preamble_skipped_not_forwarded(void) {
    static const char doc[] =
        "Latitude: 37.7749  Longitude: -122.4194\n"
        "Tile: 11369\n"
        "2023,2,34011.34,9.53,120.25,11.81,6.74,983.53\n";
    pre_clim_n = 0;
    assoc_climate_feed_t *f = assoc_climate_feed_new(on_pre_clim, NULL);
    ok(f != NULL, "preamble: feed constructed");
    if (!f) return;
    assoc_climate_feed(f, (const unsigned char *)doc, sizeof doc - 1);
    assoc_climate_feed_free(f);
    /* 2 prose preamble lines dropped, 1 data row forwarded. */
    okf(pre_clim_n == 1, "preamble: prose lines dropped, data row kept (n=%d)", pre_clim_n);
}

/* ------------------------------------------------------------------ *
 * 4. A row longer than ASSOC_MAX_LINE was silently truncated and then
 *    emitted, yielding a plausible-looking but wrong number.
 *    Must now reject the whole row and stay usable afterwards.
 * ------------------------------------------------------------------ */
static int big_n;
static void on_big(const assoc_fire_t *f, void *u) {
    (void)u;
    (void)f;
    big_n++;
}

static void test_overflow_rejects_row(void) {
    const size_t cap = 4u << 20;              /* 4MB, > 1MB line cap */
    char *doc = malloc(cap);
    if (!doc) { okf(0, "overflow: malloc"); return; }

    int p = snprintf(doc, cap, "latitude,longitude,frp\n49.9,-113.0,");
    for (int i = 0; i < (1 << 20); i++) doc[p++] = '9';   /* >1MB field */
    doc[p++] = '\n';
    /* A valid row afterwards: the parser must recover, not wedge. */
    p += snprintf(doc + p, cap - (size_t)p, "1,2,3\n");

    assoc_fire_opts_t o = { -1, NULL };
    big_n = 0;
    assoc_fire_feed_t *f = assoc_fire_feed_new(on_big, NULL, &o);
    ok(f != NULL, "overflow: feed constructed");
    if (f) {
        assoc_fire_feed(f, (const unsigned char *)doc, (size_t)p);
        assoc_fire_feed_free(f);
    }
    /* only the trailing good row must survive */
    okf(big_n == 1, "overflow: oversized row rejected, parser recovered (n=%d)", big_n);
    free(doc);
}

/* ------------------------------------------------------------------ *
 * 5. Too many columns was silently truncated (row emitted with the
 *    tail dropped). Must reject instead.
 * ------------------------------------------------------------------ */
static void test_too_many_columns_rejected(void) {
    const size_t cap = 8192;
    char *doc = malloc(cap);
    if (!doc) { okf(0, "cols: malloc"); return; }

    int p = snprintf(doc, cap, "latitude,longitude,frp");
    for (int i = 0; i < 100; i++) p += snprintf(doc + p, cap - (size_t)p, ",c%d", i);
    p += snprintf(doc + p, cap - (size_t)p, "\n49.9,-113.0,42.0");
    for (int i = 0; i < 100; i++) p += snprintf(doc + p, cap - (size_t)p, ",%d", i);
    doc[p++] = '\n';
    doc[p] = '\0';

    assoc_fire_opts_t o = { -1, NULL };
    big_n = 0;
    assoc_fire_feed_t *f = assoc_fire_feed_new(on_big, NULL, &o);
    if (f) {
        assoc_fire_feed(f, (const unsigned char *)doc, (size_t)p);
        assoc_fire_feed_free(f);
    }
    okf(big_n == 0, "cols: >ASSOC_MAX_COLS row rejected, not truncated (n=%d)", big_n);
    free(doc);
}

/* ------------------------------------------------------------------ *
 * 6. RFC4180 escaped quotes. "" inside a quoted field is one quote.
 *    The old splitter kept both, so acq_date came back as 'a""b'.
 * ------------------------------------------------------------------ */
static assoc_fire_t esc_fire;
static void on_esc(const assoc_fire_t *f, void *u) { (void)u; esc_fire = *f; }

static void feed_fire(const char *doc, size_t len) {
    assoc_fire_opts_t o = { -1, NULL };
    memset(&esc_fire, 0, sizeof esc_fire);
    assoc_fire_feed_t *f = assoc_fire_feed_new(on_esc, NULL, &o);
    if (!f) return;
    assoc_fire_feed(f, (const unsigned char *)doc, len);
    assoc_fire_feed_free(f);
}

static void test_escaped_quotes(void) {
    static const char doc[] =
        "lat,lon,acq_date,satellite,confidence,frp\n"
        "1,2,\"a\"\"b\",h,5\n";
    feed_fire(doc, sizeof doc - 1);
    ok(strcmp(esc_fire.acq_date, "a\"b") == 0,
       "quotes: \"\" collapses to one quote in acq_date");

    /* Four quote characters = a quoted field holding exactly one quote.
     * Built from explicit char values: counting backslash-escapes inside a
     * C literal is exactly how the five-quote typo got in. */
    static const char doc2[] =
        "lat,lon,acq_date,satellite,confidence,frp\n"
        "1,2,\x22\x22\x22\x22,h,5\n";
    feed_fire(doc2, sizeof doc2 - 1);
    ok(strcmp(esc_fire.acq_date, "\"") == 0 && strlen(esc_fire.acq_date) == 1,
       "quotes: \"\"\"\" decodes to exactly one quote");

    /* Odd quote counts are malformed CSV: the row must be dropped.
     * big_n is reused as the emit counter here, so reset it first. */
    static const char doc3[] =
        "lat,lon,acq_date,satellite,confidence,frp\n"
        "1,2,\x22\x22\x22,h,5\n";              /* three quotes: unterminated */
    big_n = 0;
    assoc_fire_opts_t o = { -1, NULL };
    assoc_fire_feed_t *f = assoc_fire_feed_new(on_big, NULL, &o);
    if (f) {
        assoc_fire_feed(f, (const unsigned char *)doc3, sizeof doc3 - 1);
        assoc_fire_feed_free(f);
    }
    okf(big_n == 0, "quotes: malformed odd-quote row rejected (n=%d)", big_n);
}

/* ------------------------------------------------------------------ *
 * 7. GBIF: a record whose VALUE contained the literal text "results"
 *    hijacked the naive memcmp scan and failed the whole page.
 * ------------------------------------------------------------------ */
static int occ_n;
static void on_occ(const assoc_occ_t *o, void *u) { (void)u; (void)o; occ_n++; }

static void test_gbif_decoy_results_key(void) {
    static const char page[] =
        "{\"note\":\"results\",\"results\":[{\"decimalLatitude\":50.1,"
        "\"decimalLongitude\":-112.1,\"scientificName\":\"x\"}]}";
    int kept = -1, skipped = -1;
    occ_n = 0;
    int rc = assoc_gbif_parse_page(page, sizeof page - 1, "sp", on_occ, NULL,
                                   NULL, -1, &kept, &skipped);
    ok(rc == 1 && kept == 1 && occ_n == 1,
       "gbif: value containing \"results\" does not hijack the key scan");
}

/* ------------------------------------------------------------------ *
 * 8. GBIF: a malformed tail returned -1 but left kept/skipped at 0
 *    even though callbacks had already fired. Tallies must be real.
 * ------------------------------------------------------------------ */
static void test_gbif_tallies_on_malformed_tail(void) {
    static const char page[] =
        "{\"results\":[{\"decimalLatitude\":50.1,\"decimalLongitude\":-112.1},"
        "{\"decimalLatitude\":50.2,\"decimalLongitude\":-112.2";
    int kept = -1, skipped = -1;
    occ_n = 0;
    int rc = assoc_gbif_parse_page(page, sizeof page - 1, "sp", on_occ, NULL,
                                   NULL, -1, &kept, &skipped);
    ok(rc == -1, "gbif: truncated tail reports -1");
    okf(kept == occ_n,
        "gbif: kept tally matches callbacks already fired (kept=%d fired=%d)",
        kept, occ_n);
}

/* ------------------------------------------------------------------ *
 * 9. Records with no uncertainty are always kept (design intent).
 * ------------------------------------------------------------------ */
static void test_gbif_missing_uncertainty_kept(void) {
    static const char page[] =
        "{\"results\":[{\"decimalLatitude\":50.1,\"decimalLongitude\":-112.1}]}";
    int kept = 0, skipped = 0;
    occ_n = 0;
    int rc = assoc_gbif_parse_page(page, sizeof page - 1, "sp", on_occ, NULL,
                                   NULL, 0.0 /* max_uncert_m = 0: very strict */,
                                   &kept, &skipped);
    ok(rc == 1 && kept == 1,
       "gbif: record lacking coordinateUncertaintyInMeters is kept, not dropped");
}

int main(void) {
    printf("== assoc regression suite ==\n");
    test_null_callback_rejected();
    test_headerless_positional_fallback();
    test_preamble_skipped_not_forwarded();
    test_overflow_rejects_row();
    test_too_many_columns_rejected();
    test_escaped_quotes();
    test_gbif_decoy_results_key();
    test_gbif_tallies_on_malformed_tail();
    test_gbif_missing_uncertainty_kept();

    printf("\n%d checks, %d failures\n", checks, failures);
    if (failures) {
        printf("REGRESSION SUITE FAILED\n");
        return 1;
    }
    printf("assoc regression: all pass\n");
    return 0;
}