#ifndef SNAPSHOT_OUTPUT_H
#define SNAPSHOT_OUTPUT_H

/*
 * LDFD - Output sink API
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

/* Output sinks. Kept out of snapshot.h so a parser-only user does not need
 * to see the sink vtable plumbing. */

#include "snapshot.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Chunk callback for snap_callback_output. */
typedef void (*snap_chunk_cb_t)(const snap_chunk_t *chunk, void *udata);

/* ---- raw byte sinks ---- */
extern const snap_output_t snap_file_output;     /* append raw bytes to a file */
extern const snap_output_t snap_callback_output; /* hand chunks to a C callback */

/* Wrap an already-open stream instead of opening one. config is unused.
 * The caller keeps ownership of the stream. */
snap_error_t snap_file_output_attach(void **ctx, void *fp);

/* Register the callback for a snap_callback_output ctx. */
snap_error_t snap_callback_set(void *output_ctx, snap_chunk_cb_t cb, void *udata);

/* ---- CSV rows -> newline-delimited JSON ----
 * The integration path into the Lancius trainer: header names come from the
 * CSV header row, one JSON object per data row, appended to config (the
 * path). A ":w" suffix on the path truncates instead of appending. */
snap_error_t snap_row_out_init(void **ctx, const char *config);
void         snap_row_out_set_stream(void *ctx, void *fp);
void         snap_row_out_set_header(void *ctx, char **names, int n_names);
void         snap_row_out_emit(void *ctx, const char **fields, int n_fields);
/* Ragged rows never shift values between columns: a field past the header
 * gets a synthetic `field_<n>` name and missing fields are simply absent.
 *
 * A value that parses fully as a finite number is written as a JSON number
 * in the shortest form that round-trips, so downstream code gets real
 * numbers rather than strings. Anything else -- including a literal that
 * overflows the double range, or `nan` / `inf` -- is written as a JSON
 * string, because bare `inf`/`nan` is not valid JSON. */
snap_error_t snap_row_out_close(void *ctx);
void         snap_row_out_free(void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* SNAPSHOT_OUTPUT_H */