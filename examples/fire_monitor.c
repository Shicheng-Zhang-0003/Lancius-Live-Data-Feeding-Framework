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
#include "parser_csv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

static volatile sig_atomic_t g_shutdown = 0;

static void sig_handler(int sig) { (void)sig; g_shutdown = 1; }

/* Callback for each parsed CSV row (MODIS C6.1 layout). */
static void fire_row_cb(const char **fields, int n_fields, void *udata) {
    (void)udata;
    if (n_fields < 13) return;

    char *end = NULL;
    double lat = strtod(fields[0], &end);
    if (end == fields[0]) return;
    double lon = strtod(fields[1], NULL);
    double frp = strtod(fields[11], NULL);
    const char *date = fields[5];
    const char *sat = fields[7];

    if (frp > 100.0) {  /* Only high-FRP fires */
        printf("FIRE: %.4f, %.4f | FRP: %.1f MW | %s | %s\n",
               lat, lon, frp, date, sat);
        fflush(stdout);
    }
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { fprintf(stderr, "out of memory\n"); return 1; }

    snap_source_t src = {
        .name = "FIRMS-MODIS-CA-7d",
        .url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_USA_contiguous_and_Hawaii_7d.csv",
        .auth_header = NULL,
        .format = SNAP_FMT_CSV,
        .interval_sec = 3600,  /* Every hour */
        .parser_config = NULL,
        .transform_config = NULL,
        .output_config = NULL
    };

    if (snap_ctx_add_source(ctx, &src) != SNAP_OK) {
        fprintf(stderr, "Failed to add source\n");
        snap_ctx_free(ctx);
        return 1;
    }

    /* Public API: never touch the parser's private struct. */
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, fire_row_cb, NULL);

    printf("Starting fire monitor (Ctrl+C to stop)...\n");
    printf("Source: %s\n", src.name);
    printf("URL: %s\n", src.url);
    printf("Interval: %d seconds\n\n", src.interval_sec);

    if (snap_ctx_run_once(ctx) != SNAP_OK)
        fprintf(stderr, "warning: initial fetch failed\n");

    while (!g_shutdown) {
        unsigned int left = (unsigned int)src.interval_sec;
        while (left > 0 && !g_shutdown) {
            unsigned int step = left > 1 ? 1 : left;
            sleep(step);
            left -= step;
        }
        if (g_shutdown) break;
        if (snap_ctx_run_once(ctx) != SNAP_OK)
            fprintf(stderr, "warning: fetch failed\n");
    }

    printf("\nShutting down...\n");
    snap_ctx_free(ctx);
    return 0;
}
