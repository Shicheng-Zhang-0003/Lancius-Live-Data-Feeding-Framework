/*
 * LDFD - Decompression: streaming gzip (RFC1952) and tar (ustar/pax/GNU)
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
 * Needed because the Lancius corpus is not plain text: MNIST ships as
 * .gz, CIFAR-10 as .tar.gz. Both are handled here as a streaming layer --
 * input arrives in arbitrarily-sized chunks (a curl write callback) and
 * plain bytes come out, so a multi-gigabyte archive never has to be
 * buffered whole.
 *
 * Deps: zlib only. libarchive would cover more formats but adds a heavier
 * dependency for the two shapes the corpus actually uses.
 */

/* realpath() needs _XOPEN_SOURCE/_DEFAULT_SOURCE alongside POSIX 2008. */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1

#include "decompress.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>

/* ------------------------------------------------------------------ *
 * gzip / zlib streaming inflate
 * ------------------------------------------------------------------ */

struct snap_gzip {
    z_stream zs;
    int      zs_active;      /* inflateInit succeeded */
    snap_buffer_t *out;
    int      done;           /* stream reached its end */
    int      saw_magic;      /* header validated at least once */
    unsigned char magic_buf[2];
    size_t   magic_len;      /* bytes held back awaiting the 2-byte magic */
};

static const unsigned char GZ_MAGIC[2] = { 0x1f, 0x8b };

int snap_gzip_is_gzip(const unsigned char *data, size_t len) {
    return data && len >= 2 && data[0] == GZ_MAGIC[0] && data[1] == GZ_MAGIC[1];
}

snap_gzip_t *snap_gzip_new(snap_buffer_t *out) {
    if (!out) return NULL;
    snap_gzip_t *g = calloc(1, sizeof *g);
    if (!g) return NULL;
    g->out = out;

    /* windowBits = 15 + 16 selects gzip framing automatically */
    g->zs.zalloc = Z_NULL;
    g->zs.zfree = Z_NULL;
    g->zs.opaque = Z_NULL;
    if (inflateInit2(&g->zs, 15 + 16) != Z_OK) {
        free(g);
        return NULL;
    }
    g->zs_active = 1;
    return g;
}

/* Pull whatever inflate has ready into the output buffer. Returns
 * SNAP_OK while making progress, SNAP_OK with *eof set at clean end. */
static int gzip_pump(snap_gzip_t *g, int *eof) {
    unsigned char chunk[16384];
    if (eof) *eof = 0;
    for (;;) {
        g->zs.next_out = chunk;
        g->zs.avail_out = sizeof chunk;
        int rc = inflate(&g->zs, Z_NO_FLUSH);
        if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
            return SNAP_ERR_PARSE;
        }
        size_t have = sizeof chunk - g->zs.avail_out;
        if (have > 0) {
            snap_buffer_append(g->out, chunk, have);
        }
        if (rc == Z_STREAM_END) { if (eof) *eof = 1; return SNAP_OK; }
        if (have == 0) return SNAP_OK;   /* needs more input */
    }
}

snap_error_t snap_gzip_feed(snap_gzip_t *g, const unsigned char *data, size_t len) {
    if (!g || !g->zs_active) return SNAP_ERR_CONFIG;
    if (g->done) return SNAP_OK;             /* trailing bytes after end */
    if (!data || len == 0) return SNAP_OK;

    /* Hold bytes back until the 2-byte magic is known, so a non-gzip payload
     * is rejected instead of being fed to inflate as noise. curl can hand us
     * one byte at a time, so the hold-back has to be incremental rather than
     * "reject anything shorter than 2 bytes".
     *
     * The held-back bytes are NOT dropped: inflate parses the gzip header
     * itself, so it must still see the magic. The first feed therefore goes
     * through a contiguous staging buffer of magic + payload. */
    if (!g->saw_magic) {
        while (g->magic_len < sizeof g->magic_buf && len > 0) {
            g->magic_buf[g->magic_len++] = *data++;
            len--;
        }
        /* Not yet enough to judge: a 1-byte chunk cannot be rejected. */
        if (g->magic_len < sizeof g->magic_buf) return SNAP_OK;
        if (!snap_gzip_is_gzip(g->magic_buf, g->magic_len)) return SNAP_ERR_PARSE;
        g->saw_magic = 1;

        size_t total = sizeof g->magic_buf + len;
        if (total > UINT_MAX) return SNAP_ERR_OUTPUT;   /* see snap_gzip_feed */
        unsigned char *stage = malloc(total);
        if (!stage) return SNAP_ERR_NOMEM;
        memcpy(stage, g->magic_buf, sizeof g->magic_buf);
        if (len) memcpy(stage + sizeof g->magic_buf, data, len);
        g->zs.next_in = stage;
        g->zs.avail_in = (uInt)total;
        int eof = 0;
        snap_error_t rc = gzip_pump(g, &eof);
        free(stage);
        return rc;
    }
    if (len == 0) return SNAP_OK;
    /* zlib's avail_in is 32-bit. A larger chunk would be truncated on the
     * cast, feeding inflate a prefix and losing the tail. Refuse instead. */
    if (len > UINT_MAX) return SNAP_ERR_OUTPUT;

    g->zs.next_in = (Bytef *)(uintptr_t)(const void *)data;
    g->zs.avail_in = (uInt)len;
    int eof = 0;
    return gzip_pump(g, &eof);
}

snap_error_t snap_gzip_finish(snap_gzip_t *g) {
    if (!g || !g->zs_active) return SNAP_ERR_CONFIG;
    if (g->done) return SNAP_OK;
    /* No more input is coming. Anything still pending inside inflate means
     * the archive was cut short: report it rather than accepting a partial
     * dataset as if it were complete. */
    unsigned char sink[4096];
    for (;;) {
        g->zs.next_out = sink;
        g->zs.avail_out = sizeof sink;
        int rc = inflate(&g->zs, Z_FINISH);
        size_t have = sizeof sink - g->zs.avail_out;
        if (have > 0) snap_buffer_append(g->out, sink, have);
        if (rc == Z_STREAM_END) { g->done = 1; return SNAP_OK; }
        if (rc == Z_BUF_ERROR || rc == Z_OK) return SNAP_ERR_PARSE; /* truncated */
        return SNAP_ERR_PARSE;
    }
}

void snap_gzip_free(snap_gzip_t *g) {
    if (!g) return;
    if (g->zs_active) { inflateEnd(&g->zs); g->zs_active = 0; }
    free(g);
}

/* ------------------------------------------------------------------ *
 * tar
 * ------------------------------------------------------------------ */

/*
 * Only regular files are extracted. Links, devices, directories and any
 * entry whose resolved path escapes the destination are refused: this is
 * the tar-slip class of bug, and the destination here is a data directory
 * that a training run later reads.
 */

#define TAR_BLOCK 512

static int oct_field(const char *p, size_t n, long long *out) {
    /* octal, NUL- or space-padded, possibly with a base-256 high bit */
    if (n == 0) return 0;
    if ((unsigned char)p[0] & 0x80) {
        long long v = (long long)(unsigned char)p[0] & 0x7f;
        for (size_t i = 1; i < n; i++) v = (v << 8) | (unsigned char)p[i];
        *out = v;
        return 1;
    }
    long long v = 0;
    size_t i = 0;
    while (i < n && (p[i] == ' ' || p[i] == '\0')) i++;
    for (; i < n && p[i] >= '0' && p[i] <= '7'; i++) {
        v = v * 8 + (p[i] - '0');
        if (v > (long long)1 << 62) return 0;      /* absurd size */
    }
    *out = v;
    return 1;
}

static int is_zero_block(const unsigned char *b) {
    for (int i = 0; i < TAR_BLOCK; i++) if (b[i]) return 0;
    return 1;
}

/* Create every missing component of `path`. Only called for a path already
 * proven to sit under dest_dir. */
static void mkdir_p(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof tmp, "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        mkdir(tmp, 0755);
        *p = '/';
    }
    mkdir(tmp, 0755);
}

/* typeflag: '0'/'\0' regular, 'L' GNU long name, 'x'/'g' pax headers */
static int tar_type(const unsigned char *b) {
    return b[156] ? b[156] : '0';
}

snap_error_t snap_tar_extract(snap_buffer_t *in, const char *dest_dir,
                              snap_tar_stats_t *st) {
    if (!in || !in->data || !dest_dir) return SNAP_ERR_CONFIG;
    snap_tar_stats_t local;
    if (!st) st = &local;
    memset(st, 0, sizeof *st);

    size_t pos = 0;
    char long_name[101];   /* ustar name field is 100 bytes */
    long_name[0] = '\0';
    int  have_name = 0;      /* set when a pax 'path=' overrode the ustar name */

    /* A tar archive is always a whole number of 512-byte blocks, and the
     * shortest meaningful one is two zero blocks (1024 bytes). Anything
     * shorter cannot contain a header, so the loop below would never run and
     * would report SNAP_OK with files==0 -- a silent success that discards
     * the payload. A caller that hands this a plain CSV (poll_once
     * --extract-tar does exactly that) must be told it is not an archive
     * instead of watching the download vanish. */
    if (in->len > 0 && (in->len % TAR_BLOCK) != 0) {
        st->rejected++;
        return SNAP_ERR_PARSE;
    }

    while (pos + TAR_BLOCK <= in->len) {
        const unsigned char *hdr = in->data + pos;

        if (is_zero_block(hdr)) {
            /* Two zero blocks end the archive. */
            if (pos + 2 * TAR_BLOCK <= in->len &&
                is_zero_block(in->data + pos + TAR_BLOCK)) {
                return SNAP_OK;
            }
            pos += TAR_BLOCK;
            continue;
        }

        /* header checksum: sum of bytes with the checksum field read as spaces */
        long long stored_cksum = 0;
        if (!oct_field((const char *)hdr + 148, 8, &stored_cksum)) {
            st->rejected++;
            return SNAP_ERR_PARSE;
        }
        unsigned int sum = 0;
        for (int i = 0; i < TAR_BLOCK; i++) {
            sum += (i >= 148 && i < 156) ? ' ' : hdr[i];
        }
        if ((long long)sum != stored_cksum) {
            /* A corrupt header means every following offset is untrustworthy. */
            st->rejected++;
            return SNAP_ERR_PARSE;
        }

        long long size = 0;
        if (!oct_field((const char *)hdr + 124, 12, &size) || size < 0) {
            st->rejected++;
            return SNAP_ERR_PARSE;
        }
        int type = tar_type(hdr);
        /* Round the member size up to a 512 boundary without wrapping:
         * (size + 511) overflows for a size near SIZE_MAX and would yield a
         * tiny `padded`, desynchronising every later offset. */
        if ((unsigned long long)size > (unsigned long long)(SIZE_MAX - TAR_BLOCK)) {
            st->rejected++;
            return SNAP_ERR_PARSE;
        }
        size_t data_off = pos + TAR_BLOCK;
        size_t padded = ((size_t)size + TAR_BLOCK - 1) / TAR_BLOCK * TAR_BLOCK;
        if (padded > in->len - pos) {
            /* The declared member runs past the end of the image. */
            st->rejected++;
            return SNAP_ERR_PARSE;
        }

        char name[257];
        memcpy(name, hdr, 100);
        name[100] = '\0';
        char prefix[156];
        memcpy(prefix, hdr + 345, 155);
        prefix[155] = '\0';
        if (have_name) {
            have_name = 0;                 /* pax path= already filled name */
        } else if (long_name[0]) {
            memcpy(name, long_name, sizeof long_name);
            long_name[0] = '\0';
        }
        if (prefix[0] && !strchr(name, '/')) {
            char joined[413];
            int jn = snprintf(joined, sizeof joined, "%s/%s", prefix, name);
            if (jn < 0 || (size_t)jn >= sizeof joined) {
                st->rejected++;              /* refuse, do not truncate a path */
                pos = data_off + padded;
                continue;
            }
            memcpy(name, joined, (size_t)jn + 1);
        }

        if (type == 'L') {                      /* GNU long name */
            if (data_off + (size_t)size <= in->len &&
                (size_t)size < sizeof long_name) {
                memcpy(long_name, in->data + data_off, (size_t)size);
                long_name[size] = '\0';
            }
            pos = data_off + padded;
            continue;
        }
        if (type == 'x' || type == 'g') {       /* pax extended header */
            /* pax records are "LEN KEY=VALUE\n". The only one that matters
             * for extraction is `path=`, which is how a >100-char member name
             * is carried in a modern (pax) archive. Read it, then still skip
             * the header itself. */
            if (type == 'x' && data_off + (size_t)size <= in->len && size > 0) {
                const char *rec = (const char *)(in->data + data_off);
                size_t rlen = (size_t)size;
                size_t off = 0;
                while (off < rlen) {
                    /* parse the decimal length prefix */
                    size_t v = 0, q = off;
                    while (q < rlen && rec[q] >= '0' && rec[q] <= '9') {
                        v = v * 10 + (size_t)(rec[q] - '0');
                        q++;
                    }
                    if (q == off || q >= rlen || rec[q] != ' ' || v == 0) break;
                    q++;                                   /* skip the space */
                    if (off + v > rlen) break;
                    size_t kvlen = v - (q - off) - 1;      /* minus digits+space */
                    if (kvlen >= 5 && memcmp(rec + q, "path=", 5) == 0) {
                        size_t vlen = kvlen - 5;
                        if (vlen < sizeof name) {
                            memcpy(name, rec + q + 5, vlen);
                            name[vlen] = '\0';
                            have_name = 1;
                        }
                    }
                    off += v;
                }
            }
            pos = data_off + padded;
            continue;
        }
        if (type != '0' && type != '\0') {      /* dir, link, device, ... */
            st->skipped++;
            pos = data_off + padded;
            continue;
        }

        /* refuse absolute paths and any traversal */
        const char *rel = name;
        while (*rel == '/') rel++;
        if (strstr(rel, "..") != NULL) {
            st->rejected++;
            pos = data_off + padded;
            continue;
        }
        if (data_off + (size_t)size > in->len) {
            st->rejected++;
            return SNAP_ERR_PARSE;              /* truncated member */
        }

        char out_path[PATH_MAX];
        int n = snprintf(out_path, sizeof out_path, "%s/%s", dest_dir, rel);
        if (n < 0 || (size_t)n >= sizeof out_path) {
            st->rejected++;
            pos = data_off + padded;
            continue;
        }

        /* Defence in depth: the joined path must still live under dest_dir. */
        char resolved[PATH_MAX];
        if (!realpath(dest_dir, resolved)) {
            st->rejected++;
            pos = data_off + padded;
            continue;
        }
        char dir_of[PATH_MAX];
        snprintf(dir_of, sizeof dir_of, "%s", out_path);
        char *slash = strrchr(dir_of, '/');
        if (slash) { *slash = '\0'; } else { dir_of[0] = '.'; }
        char rdir[PATH_MAX];
        if (!realpath(dir_of, rdir)) {
            /* parent may not exist yet; fall back to a lexical check */
            size_t dl = strlen(resolved);
            if (strncmp(out_path, resolved, dl) != 0 || out_path[dl] != '/') {
                st->rejected++;
                pos = data_off + padded;
                continue;
            }
        } else {
            size_t rl = strlen(rdir);
            if (strncmp(out_path, rdir, rl) != 0 || out_path[rl] != '/') {
                st->rejected++;
                pos = data_off + padded;
                continue;
            }
        }

        FILE *f = fopen(out_path, "wb");
        if (!f) {
            /* A real tar creates the member's parent directories. */
            mkdir_p(dir_of);
            f = fopen(out_path, "wb");
        }
        if (!f) {
            st->rejected++;
            pos = data_off + padded;
            continue;
        }
        size_t want = (size_t)size;
        size_t wrote = want ? fwrite(in->data + data_off, 1, want, f) : 0;
        int flushed = fflush(f);
        fclose(f);
        if (wrote != want || flushed != 0) {
            st->rejected++;
            pos = data_off + padded;
            continue;
        }
        st->files++;
        st->bytes += size;
        pos = data_off + padded;
    }
    return SNAP_OK;
}