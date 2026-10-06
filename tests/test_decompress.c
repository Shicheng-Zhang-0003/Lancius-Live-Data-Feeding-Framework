/*
 * LDFD - Decompression tests (gzip streaming + tar extraction)
 * Copyright (C) 2026
 *
 * Build:
 *   gcc -std=c17 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -Iinclude \
 *       tests/test_decompress.c src/decompress.c src/buffer.c -lz \
 *       -o /tmp/test_decompress && /tmp/test_decompress
 *
 * No network. Builds its own archives with zlib, so the fixtures cannot
 * drift from what the code expects.
 */

#define _POSIX_C_SOURCE 200809L

#include "decompress.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>

static int failures;
static int checks;

static void ok(int cond, const char *what) {
    checks++;
    printf("%s %s\n", cond ? "PASS" : "FAIL", what);
    if (!cond) failures++;
}

/* ---------------- gzip ---------------- */

/* Compress `text` into a gzip stream via zlib's gzip wrapper. */
static unsigned char *make_gzip(const char *text, size_t *out_len) {
    z_stream zs;
    memset(&zs, 0, sizeof zs);
    if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) return NULL;
    size_t in_len = strlen(text);
    size_t cap = in_len + in_len / 2 + 1024;
    unsigned char *out = malloc(cap);
    if (!out) { deflateEnd(&zs); return NULL; }
    zs.next_in = (Bytef *)(uintptr_t)(const void *)text;
    zs.avail_in = (uInt)in_len;
    zs.next_out = out;
    zs.avail_out = (uInt)cap;
    int rc = deflate(&zs, Z_FINISH);
    size_t have = cap - zs.avail_out;
    deflateEnd(&zs);
    if (rc != Z_STREAM_END) { free(out); return NULL; }
    *out_len = have;
    return out;
}

static void test_gzip_roundtrip(void) {
    static const char text[] =
        "latitude,longitude,frp\n"
        "49.9000,-113.0000,15.46\n"
        "50.1000,-112.5000,42.00\n";

    size_t gz_len = 0;
    unsigned char *gz = make_gzip(text, &gz_len);
    if (!gz) { ok(0, "gzip: fixture built"); return; }

    ok(snap_gzip_is_gzip(gz, gz_len), "gzip: magic recognised");
    ok(!snap_gzip_is_gzip((const unsigned char *)"not gzip at all", 16),
       "gzip: non-gzip payload rejected by the magic check");

    snap_buffer_t out;
    snap_buffer_init(&out, 64);
    snap_gzip_t *g = snap_gzip_new(&out);
    ok(g != NULL, "gzip: decoder created");
    if (g) {
        ok(snap_gzip_feed(g, gz, gz_len) == SNAP_OK, "gzip: feed whole stream");
        ok(snap_gzip_finish(g) == SNAP_OK, "gzip: finish reports clean end");
        snap_gzip_free(g);
    }
    ok(out.len == strlen(text) && memcmp(out.data, text, out.len) == 0,
       "gzip: round-tripped payload is byte-identical");
    snap_buffer_free(&out);
    free(gz);
}

/* The corpus arrives in curl-sized chunks, so chunk boundaries land in
 * arbitrary places including mid-deflate-block. */
static void test_gzip_split_chunks(void) {
    static const char text[] =
        "year,yday,dayl (s),prcp (mm/day),srad (W/m^2),tmax (deg c),tmin (deg c),vp (Pa)\n"
        "2023,1,34011.34,9.53,120.25,11.81,6.74,983.53\n"
        "2023,2,34500.01,0.00,150.10,14.02,7.11,1002.44\n";
    size_t gz_len = 0;
    unsigned char *gz = make_gzip(text, &gz_len);
    if (!gz) { ok(0, "gzip split: fixture built"); return; }

    for (size_t chunk = 1; chunk <= 7; chunk++) {
        snap_buffer_t out;
        snap_buffer_init(&out, 32);
        snap_gzip_t *g = snap_gzip_new(&out);
        snap_error_t rc = SNAP_OK;
        for (size_t off = 0; off < gz_len && rc == SNAP_OK; off += chunk) {
            size_t n = gz_len - off < chunk ? gz_len - off : chunk;
            rc = snap_gzip_feed(g, gz + off, n);
        }
        if (rc == SNAP_OK) rc = snap_gzip_finish(g);
        int same = (out.len == strlen(text) &&
                    memcmp(out.data, text, out.len) == 0);
        if (!same) printf("   (chunk=%zu len=%zu)\n", chunk, out.len);
        ok(rc == SNAP_OK && same, "gzip: survives small chunk sizes");
        snap_gzip_free(g);
        snap_buffer_free(&out);
    }
    free(gz);
}

/* A half-downloaded archive must fail, not yield a partial dataset that
 * looks complete. */
static void test_gzip_truncated_detected(void) {
    static const char text[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    size_t gz_len = 0;
    unsigned char *gz = make_gzip(text, &gz_len);
    if (!gz) { ok(0, "gzip trunc: fixture built"); return; }

    snap_buffer_t out;
    snap_buffer_init(&out, 32);
    snap_gzip_t *g = snap_gzip_new(&out);
    /* feed everything except the final bytes: trailer/flush is missing */
    size_t cut = gz_len > 6 ? gz_len - 6 : 0;
    snap_gzip_feed(g, gz, cut);
    snap_error_t rc = snap_gzip_finish(g);
    ok(rc != SNAP_OK, "gzip: truncated stream is reported as an error");
    snap_gzip_free(g);
    snap_buffer_free(&out);
    free(gz);
}

/* ---------------- tar ---------------- */

static void put_oct(char *dst, size_t width, long long v) {
    /* ustar numeric field: zero-padded octal, NUL terminated.
     * The whole field is zeroed first -- leaving the leading bytes
     * uninitialised makes the checksum non-deterministic. */
    memset(dst, '0', width);
    dst[width - 1] = '\0';
    size_t i = width - 1;
    for (; i > 0 && v > 0; i--) {
        dst[i] = (char)('0' + (int)(v & 7));
        v >>= 3;
    }
}

static void tar_checksum(unsigned char *blk) {
    memset(blk + 148, ' ', 8);
    unsigned int sum = 0;
    for (int i = 0; i < 512; i++) sum += blk[i];
    char oct[9];
    memset(oct, 0, sizeof oct);
    put_oct(oct, 8, (long long)sum);
    memcpy(blk + 148, oct, 8);
}

/* Append one tar member. typeflag '0' regular, '5' directory. */
static void tar_add(snap_buffer_t *t, const char *name, const char *data,
                    size_t len, char type) {
    unsigned char blk[512];
    memset(blk, 0, sizeof blk);
    snprintf((char *)blk, 100, "%s", name);
    put_oct((char *)blk + 100, 8, 0644);          /* mode */
    put_oct((char *)blk + 108, 8, 0);             /* uid */
    put_oct((char *)blk + 116, 8, 0);             /* gid */
    put_oct((char *)blk + 124, 12, (long long)len);
    put_oct((char *)blk + 136, 12, 0);            /* mtime */
    memset(blk + 148, ' ', 8);
    blk[156] = (unsigned char)type;
    /* ustar magic + version at 257; the PREFIX field is 345 and must stay
     * empty (writing to it makes the member path "<prefix>/<name>"). */
    memcpy(blk + 257, "ustar\0" "00", 8);
    tar_checksum(blk);
    snap_buffer_append(t, blk, sizeof blk);
    if (len) {
        snap_buffer_append(t, (const uint8_t *)data, len);
        size_t pad = (512 - (len % 512)) % 512;
        unsigned char z[512];
        memset(z, 0, sizeof z);
        if (pad) snap_buffer_append(t, z, pad);
    }
}

static void tar_end(snap_buffer_t *t) {
    unsigned char z[1024];
    memset(z, 0, sizeof z);
    snap_buffer_append(t, z, sizeof z);
}

static int file_has(const char *dir, const char *rel, const char *expect) {
    char p[512];
    snprintf(p, sizeof p, "%s/%s", dir, rel);
    FILE *f = fopen(p, "rb");
    if (!f) return 0;
    char buf[256] = { 0 };
    size_t n = fread(buf, 1, sizeof buf - 1, f);
    fclose(f);
    return n == strlen(expect) && memcmp(buf, expect, n) == 0;
}

static int file_exists(const char *dir, const char *rel) {
    char p[512];
    snprintf(p, sizeof p, "%s/%s", dir, rel);
    FILE *f = fopen(p, "rb");
    if (!f) return 0;
    fclose(f);
    return 1;
}

static char g_dest[256];

static void make_dest(void) {
    snprintf(g_dest, sizeof g_dest, "/tmp/ldfd_tar_XXXXXX");
    if (!mkdtemp(g_dest)) { g_dest[0] = '\0'; }
}

static void test_tar_extract(void) {
    make_dest();
    if (!g_dest[0]) { ok(0, "tar: dest dir created"); return; }

    snap_buffer_t t;
    snap_buffer_init(&t, 2048);
    tar_add(&t, "cifar-10-batches-bin/", NULL, 0, '5');
    tar_add(&t, "cifar-10-batches-bin/data_batch_1.bin", "BATCHONE", 8, '0');
    tar_add(&t, "cifar-10-batches-bin/data_batch_2.bin", "BATCHTWO", 8, '0');
    tar_end(&t);

    snap_tar_stats_t st;
    snap_error_t rc = snap_tar_extract(&t, g_dest, &st);
    ok(rc == SNAP_OK, "tar: extraction succeeds");
    ok(st.files == 2, "tar: two regular files written");
    ok(st.skipped == 1, "tar: directory member skipped, not extracted");
    ok(file_has(g_dest, "cifar-10-batches-bin/data_batch_1.bin", "BATCHONE"),
       "tar: file 1 contents correct");
    ok(file_has(g_dest, "cifar-10-batches-bin/data_batch_2.bin", "BATCHTWO"),
       "tar: file 2 contents correct");
    snap_buffer_free(&t);
}

/* tar-slip: a member named ../escape must never land outside dest_dir.
 * An absolute member is stripped to a relative path and kept INSIDE
 * dest_dir (what GNU tar does), so containment -- not rejection -- is the
 * property that matters. */
static void test_tar_slip_refused(void) {
    make_dest();
    if (!g_dest[0]) { ok(0, "slip: dest dir created"); return; }

    snap_buffer_t t;
    snap_buffer_init(&t, 2048);
    tar_add(&t, "../../ldfd_escape.txt", "PWNED", 5, '0');
    tar_add(&t, "/etc/ldfd_abs.txt", "PWNED", 5, '0');
    tar_add(&t, "sub/../../escape2.txt", "PWNED", 5, '0');
    tar_add(&t, "ok.txt", "FINE", 4, '0');
    tar_end(&t);

    snap_tar_stats_t st;
    snap_error_t rc = snap_tar_extract(&t, g_dest, &st);
    ok(rc == SNAP_OK, "slip: extraction completes without error");
    ok(st.rejected == 2, "slip: the two '..' members were rejected");
    ok(!file_exists(g_dest, "ldfd_escape.txt"),
       "slip: ../ member did not land in the destination");
    ok(!file_exists(g_dest, "escape2.txt"),
       "slip: mid-path traversal did not land in the destination");
    ok(file_has(g_dest, "etc/ldfd_abs.txt", "PWNED"),
       "slip: absolute member was contained under dest_dir");
    ok(file_has(g_dest, "ok.txt", "FINE"),
       "slip: ordinary member extracted normally");
    ok(!file_exists("/tmp", "ldfd_escape.txt"),
       "slip: nothing written outside the destination");
    remove("/tmp/ldfd_escape.txt");
    remove("/etc/ldfd_abs.txt");

    snap_buffer_free(&t);
}

/* A bad checksum invalidates every later offset, so the walk must stop
 * rather than emit whatever bytes happen to follow. */
static void test_tar_bad_checksum(void) {
    make_dest();
    if (!g_dest[0]) { ok(0, "cksum: dest dir created"); return; }

    snap_buffer_t t;
    snap_buffer_init(&t, 2048);
    tar_add(&t, "first.bin", "AAA", 3, '0');
    /* corrupt the second header's checksum field */
    tar_add(&t, "second.bin", "BBB", 3, '0');
    t.data[512 + 512 + 148] = '9';
    tar_end(&t);

    snap_tar_stats_t st;
    snap_error_t rc = snap_tar_extract(&t, g_dest, &st);
    ok(rc == SNAP_ERR_PARSE, "cksum: corrupt header stops the walk");
    ok(file_has(g_dest, "first.bin", "AAA"),
       "cksum: member before the corruption was still written");

    snap_buffer_free(&t);
}

static void test_tar_truncated(void) {
    make_dest();
    if (!g_dest[0]) { ok(0, "trunc: dest dir created"); return; }

    /* Header claims 2000 bytes of data but only one 512-byte block is kept,
     * so the member is short of what its own header declares. */
    static const char big[2000] = { 0 };
    snap_buffer_t t;
    snap_buffer_init(&t, 4096);
    tar_add(&t, "big.bin", big, sizeof big, '0');
    t.len = 512 + 512;

    snap_tar_stats_t st;
    snap_error_t rc = snap_tar_extract(&t, g_dest, &st);
    ok(rc == SNAP_ERR_PARSE, "trunc: member shorter than its header claims");
    ok(!file_exists(g_dest, "big.bin"),
       "trunc: the short member was not written as if complete");
    snap_buffer_free(&t);
}

/* A payload that is not block-aligned cannot be a tar. It used to return
 * SNAP_OK with files==0, so poll_once --extract-tar on a plain CSV printed
 * "extracted 0 file(s)" and wrote nothing: the download vanished with no
 * error. A real archive is always a whole number of 512-byte blocks. */
static void test_tar_not_block_aligned(void) {
    make_dest();
    if (!g_dest[0]) { ok(0, "align: dest dir created"); return; }

    static const char csv[] = "latitude,longitude\n32.5,-81.5\n";
    snap_buffer_t b;
    snap_buffer_init(&b, 128);
    snap_buffer_append(&b, (const uint8_t *)csv, sizeof csv - 1);

    snap_tar_stats_t st;
    snap_error_t rc = snap_tar_extract(&b, g_dest, &st);
    ok(rc == SNAP_ERR_PARSE, "align: unaligned payload rejected as not-a-tar");
    ok(st.files == 0, "align: no members reported");
    snap_buffer_free(&b);

    /* One byte short of a block is still not an archive. */
    snap_buffer_init(&b, 1024);
    for (int i = 0; i < 511; i++) snap_buffer_append(&b, (const uint8_t *)"x", 1);
    rc = snap_tar_extract(&b, g_dest, &st);
    ok(rc == SNAP_ERR_PARSE, "align: 511-byte payload rejected");
    snap_buffer_free(&b);
}

/* A genuine empty archive (two zero blocks) is aligned and must still be
 * accepted: rejecting it would break real empty tarballs. */
static void test_tar_empty_archive_ok(void) {
    make_dest();
    if (!g_dest[0]) { ok(0, "emptytar: dest dir created"); return; }

    snap_buffer_t z;
    snap_buffer_init(&z, 2048);
    unsigned char zero[1024];
    memset(zero, 0, sizeof zero);
    snap_buffer_append(&z, zero, sizeof zero);

    snap_tar_stats_t st;
    snap_error_t rc = snap_tar_extract(&z, g_dest, &st);
    ok(rc == SNAP_OK, "emptytar: 1024 zero blocks accepted");
    ok(st.files == 0, "emptytar: reports zero members, not an error");
    snap_buffer_free(&z);
}

int main(void) {
    printf("== decompression suite ==\n");
    test_gzip_roundtrip();
    test_gzip_split_chunks();
    test_gzip_truncated_detected();
    test_tar_extract();
    test_tar_slip_refused();
    test_tar_bad_checksum();
    test_tar_truncated();
    test_tar_not_block_aligned();
    test_tar_empty_archive_ok();

    printf("\n%d checks, %d failures\n", checks, failures);
    if (failures) {
        printf("DECOMPRESS SUITE FAILED\n");
        return 1;
    }
    printf("decompress: all pass\n");
    return 0;
}