/*
 * LDFD - Example: one-shot fetch to disk, with gzip and tar handling
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
 * The shape the Lancius dataset layer uses: fetch once, land the bytes,
 * and transparently unpack .gz / .tar.gz so MNIST and CIFAR-10 arrive as
 * the raw files train_mnist.c and train_cifar10.c already expect.
 *
 *   poll_once <url> <dest-dir> [strip-tar]
 *
 * Any file the server sends is written to <dest-dir>/<basename>. If the
 * payload is gzip it is inflated on the way through; if it is a tar it is
 * extracted into <dest-dir>. A truncated download fails loudly instead of
 * leaving a half-file that training would happily read.
 */

#include "snapshot.h"
#include "decompress.h"
#include "output.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* Collects the fetched body, decompressing on the way if it is gzip. */
typedef struct {
    snap_buffer_t raw;      /* bytes exactly as received */
    snap_buffer_t plain;    /* inflated bytes (aliases raw when not gzip) */
    snap_gzip_t *gz;
    int was_gzip;
} collector_t;

/* Parser that hands every chunk to the collector instead of parsing it. */
static snap_error_t collect_feed(void *ctx, const snap_chunk_t *chunk) {
    collector_t *c = ctx;
    if (!c || !chunk) return SNAP_ERR_CONFIG;
    if (chunk->len == 0) return SNAP_OK;
    snap_buffer_append(&c->raw, chunk->data, chunk->len);
    if (c->was_gzip && c->gz) {
        return snap_gzip_feed(c->gz, chunk->data, chunk->len);
    }
    return SNAP_OK;
}

static snap_error_t collect_flush(void *ctx) {
    collector_t *c = ctx;
    if (c && c->was_gzip && c->gz) return snap_gzip_finish(c->gz);
    return SNAP_OK;
}

static void collect_free(void *ctx) {
    collector_t *c = ctx;
    if (!c) return;
    if (c->gz) snap_gzip_free(c->gz);
    snap_buffer_free(&c->raw);
    if (c->was_gzip) snap_buffer_free(&c->plain);
    free(c);
}

static const char *basename_of(const char *url) {
    const char *slash = strrchr(url, '/');
    const char *name = slash ? slash + 1 : url;
    /* strip a query string */
    static char buf[256];
    size_t i = 0;
    for (; name[i] && name[i] != '?' && i < sizeof buf - 1; i++) buf[i] = name[i];
    buf[i] = '\0';
    return buf[0] ? buf : "download.bin";
}

/* Create dest and any missing parents. Without this, pointing the example at
 * a not-yet-existing directory failed at fopen with a bare "cannot write",
 * which reads like a permissions problem rather than a missing directory. */
static int mkdir_p(const char *path) {
    char tmp[1024];
    size_t n = strlen(path);
    if (n == 0 || n >= sizeof tmp) return 0;
    memcpy(tmp, path, n + 1);
    for (size_t i = 1; i < n; i++) {
        if (tmp[i] != '/') continue;
        tmp[i] = '\0';
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 0;
        tmp[i] = '/';
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 0;
    return 1;
}

static int write_file(const char *path, const unsigned char *data, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    size_t w = len ? fwrite(data, 1, len, f) : 0;
    int flushed = fflush(f);
    fclose(f);
    return w == len && flushed == 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
                "usage: %s <url> <dest-dir> [--extract-tar]\n"
                "  fetches <url> once into <dest-dir>, inflating gzip and\n"
                "  extracting tar when present.\n", argv[0]);
        return 2;
    }
    const char *url = argv[1];
    const char *dest = argv[2];
    if (!mkdir_p(dest)) {
        fprintf(stderr, "cannot create %s\n", dest);
        return 1;
    }
    int extract_tar = 0;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--extract-tar") == 0) extract_tar = 1;
    }

    collector_t *c = calloc(1, sizeof *c);
    if (!c) { fprintf(stderr, "out of memory\n"); return 1; }
    snap_buffer_init(&c->raw, 65536);

    snap_parser_t sink;
    memset(&sink, 0, sizeof sink);
    sink.feed = collect_feed;
    sink.flush = collect_flush;
    sink.free = collect_free;

    /* First do a plain fetch so gzip can be detected from the magic bytes
     * before any inflate is attempted. */
    snap_error_t rc = snap_fetch_once(url, NULL, &sink, c);
    if (rc != SNAP_OK) {
        fprintf(stderr, "fetch failed (%d): %s\n", rc, url);
        collect_free(c);
        return 1;
    }

    c->was_gzip = snap_gzip_is_gzip(c->raw.data, c->raw.len);
    if (c->was_gzip) {
        /* Re-run through the decoder now that we know it is gzip. */
        snap_buffer_init(&c->plain, 65536);
        c->gz = snap_gzip_new(&c->plain);
        if (!c->gz) {
            fprintf(stderr, "cannot start gzip decoder\n");
            collect_free(c);
            return 1;
        }
        rc = snap_gzip_feed(c->gz, c->raw.data, c->raw.len);
        if (rc == SNAP_OK) rc = snap_gzip_finish(c->gz);
        if (rc != SNAP_OK) {
            fprintf(stderr, "gzip stream is corrupt or truncated: not writing\n");
            collect_free(c);
            return 1;
        }
    }

    const snap_buffer_t *body = c->was_gzip ? &c->plain : &c->raw;
    const char *name = basename_of(url);
    char path[1024];
    snprintf(path, sizeof path, "%s/%s", dest, name);

    /* A tar.gz lands as its original archive; the caller unpacks it. */
    if (extract_tar) {
        snap_tar_stats_t st;
        memset(&st, 0, sizeof st);
        /* snap_tar_extract takes a non-const buffer; body is ours to hand over */
        rc = snap_tar_extract((snap_buffer_t *)(uintptr_t)(void *)body, dest, &st);
        if (rc != SNAP_OK) {
            /* Not a tar (a plain CSV is the common case). Write the download
             * anyway rather than discarding it, so --extract-tar on a
             * non-archive still produces the data the user asked for. */
            fprintf(stderr, "%s is not a tar archive; writing it as-is\n", name);
            if (!write_file(path, body->data, body->len)) {
                fprintf(stderr, "cannot write %s\n", path);
                collect_free(c);
                return 1;
            }
            printf("wrote %s (%zu bytes%s)\n", path, body->len,
                   c->was_gzip ? ", inflated" : "");
            collect_free(c);
            return 0;
        }
        printf("extracted %lld file(s), %lld bytes from %s\n",
               st.files, st.bytes, name);
        printf("  skipped=%lld rejected=%lld\n", st.skipped, st.rejected);
    } else if (!write_file(path, body->data, body->len)) {
        fprintf(stderr, "cannot write %s\n", path);
        collect_free(c);
        return 1;
    } else {
        printf("wrote %s (%zu bytes%s)\n", path, body->len,
               c->was_gzip ? ", inflated" : "");
    }

    collect_free(c);
    return 0;
}