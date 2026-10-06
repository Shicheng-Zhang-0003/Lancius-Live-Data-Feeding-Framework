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

#define DEFAULT_CAP 4096
/* Refuse allocations beyond this. The fetch path already caps at 2GB; this
 * bounds every other caller so a bad length cannot ask for gigabytes. */
#define SNAP_BUF_MAX_CAP ((size_t)1 << 40)   /* 1 TiB */

void snap_buffer_init(snap_buffer_t *buf, size_t initial_cap) {
    if (!buf) return;
    buf->cap = initial_cap > 0 ? initial_cap : DEFAULT_CAP;
    if (buf->cap > SNAP_BUF_MAX_CAP) buf->cap = SNAP_BUF_MAX_CAP;
    buf->len = 0;
    buf->data = malloc(buf->cap);
    if (!buf->data) buf->cap = 0;
}

void snap_buffer_append(snap_buffer_t *buf, const uint8_t *data, size_t len) {
    if (!buf || !data || len == 0) return;
    if (!buf->data) return; /* prior OOM: stay empty rather than crash */

    /* Overflow-safe capacity computation. `buf->len + len` and `new_cap *= 2`
     * could both wrap: a wrapped sum skips the growth entirely (then memcpy
     * writes past the end), and a wrapped doubling spins forever. Compute in
     * the remaining space and refuse anything above the cap. */
    if (len > SNAP_BUF_MAX_CAP - buf->len) return;   /* absurd request */

    size_t need = buf->len + len;
    if (need > buf->cap) {
        size_t new_cap = buf->cap ? buf->cap : DEFAULT_CAP;
        while (new_cap < need) {
            if (new_cap > SNAP_BUF_MAX_CAP / 2) { new_cap = SNAP_BUF_MAX_CAP; break; }
            new_cap *= 2;
        }
        if (new_cap < need) return;                  /* cannot represent */
        uint8_t *new_data = realloc(buf->data, new_cap);
        if (!new_data) return; /* keep old buffer intact on OOM */
        buf->data = new_data;
        buf->cap = new_cap;
    }
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
}

void snap_buffer_reset(snap_buffer_t *buf) {
    if (!buf) return;
    buf->len = 0;
}

void snap_buffer_free(snap_buffer_t *buf) {
    if (!buf) return;
    free(buf->data);
    buf->data = NULL;
    buf->len = buf->cap = 0;
}