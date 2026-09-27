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

void snap_buffer_init(snap_buffer_t *buf, size_t initial_cap) {
    buf->cap = initial_cap > 0 ? initial_cap : DEFAULT_CAP;
    buf->len = 0;
    buf->data = malloc(buf->cap);
    if (!buf->data) buf->cap = 0;
}

void snap_buffer_append(snap_buffer_t *buf, const uint8_t *data, size_t len) {
    if (buf->len + len > buf->cap) {
        size_t new_cap = buf->cap * 2;
        while (buf->len + len > new_cap) new_cap *= 2;
        uint8_t *new_data = realloc(buf->data, new_cap);
        if (!new_data) return;
        buf->data = new_data;
        buf->cap = new_cap;
    }
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
}

void snap_buffer_reset(snap_buffer_t *buf) {
    buf->len = 0;
}

void snap_buffer_free(snap_buffer_t *buf) {
    free(buf->data);
    buf->data = NULL;
    buf->len = buf->cap = 0;
}