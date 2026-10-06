#ifndef SNAPSHOT_DECOMPRESS_H
#define SNAPSHOT_DECOMPRESS_H

/*
 * LDFD - Decompression API
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
 * Streaming gzip and in-memory tar. Present because the Lancius corpus
 * ships MNIST as .gz and CIFAR-10 as .tar.gz, so the fetch path cannot
 * assume plain text.
 */

#include "snapshot.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct snap_gzip snap_gzip_t;

/* Cheap check: does this payload start with the gzip magic? */
int snap_gzip_is_gzip(const unsigned char *data, size_t len);

/* Decompress gzip chunks into `out` (which the caller owns and reuses).
 * feed() may be called with arbitrarily-sized, arbitrarily-split chunks.
 * finish() reports a truncated stream as an error rather than returning a
 * partial dataset. */
snap_gzip_t *snap_gzip_new(snap_buffer_t *out);
snap_error_t snap_gzip_feed(snap_gzip_t *g, const unsigned char *data, size_t len);
snap_error_t snap_gzip_finish(snap_gzip_t *g);
void         snap_gzip_free(snap_gzip_t *g);

typedef struct {
    long long files;      /* regular files written */
    long long bytes;      /* bytes written */
    long long skipped;    /* non-regular entries (dirs, links, devices) */
    long long rejected;   /* unsafe or unparseable entries */
} snap_tar_stats_t;

/*
 * Extract regular files from an in-memory tar image into dest_dir.
 * Non-regular members are skipped and counted; members whose path escapes
 * dest_dir (absolute, or containing ..) are rejected. A bad header
 * checksum stops the walk, because every later offset depends on it.
 */
snap_error_t snap_tar_extract(snap_buffer_t *in, const char *dest_dir,
                              snap_tar_stats_t *st);

#ifdef __cplusplus
}
#endif

#endif /* SNAPSHOT_DECOMPRESS_H */