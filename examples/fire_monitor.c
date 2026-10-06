/*
 * LDFD - Example: continuous FIRMS active-fire monitor
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
 * One source, polled on an interval, forever.
 *
 * This used to hand-roll its own `while (!stop) { sleep(interval); fetch(); }`
 * loop, because interval_sec was parsed but read by nothing. The scheduler
 * now does the waiting, so this example just registers the source and
 * blocks on a signal. See examples/poll_once.c for the opposite shape.
 */

#include "snapshot.h"
#include "parser_csv.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t g_shutdown = 0;
static void on_signal(int sig) { (void)sig; g_shutdown = 1; }

#define FIRMS_URL \
    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/" \
    "MODIS_C6_1_USA_contiguous_and_Hawaii_7d.csv"

/* MODIS C6.1 column order (1-based positions used below). */
enum { C_LAT = 0, C_LON, C_ACQ_DATE, C_ACQ_TIME, C_SAT, C_CONF, C_FRP };

static void on_row(const char **f, int n, void *udata) {
    (void)udata;
    if (n < 13) return;                       /* short row: not MODIS */

    char *end = NULL;
    double lat = strtod(f[C_LAT], &end);
    if (end == f[C_LAT]) return;               /* header replay / junk */
    double lon = strtod(f[C_LON], NULL);
    double frp = strtod(f[C_FRP], NULL);

    if (frp > 100.0) {                        /* only high-FRP fires */
        printf("FIRE %.4f,%.4f | FRP %.1f MW | %s %s | sat %s\n",
               lat, lon, frp, f[C_ACQ_DATE], f[C_ACQ_TIME], f[C_SAT]);
        fflush(stdout);
    }
}

int main(int argc, char **argv) {
    long interval = 3600;
    const char *url = FIRMS_URL;
    if (argc > 1) interval = strtol(argv[1], NULL, 10);
    if (argc > 2) url = argv[2];
    if (interval < 1) interval = 1;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { fprintf(stderr, "out of memory\n"); return 1; }

    snap_source_t src;
    memset(&src, 0, sizeof src);
    src.name = "FIRMS-MODIS-7d";
    src.url = (char *)url;
    src.format = SNAP_FMT_CSV;
    src.interval_sec = (int)interval;          /* now actually rescheduled */

    if (snap_ctx_add_source(ctx, &src) != SNAP_OK) {
        fprintf(stderr, "failed to add source\n");
        snap_ctx_free(ctx);
        return 1;
    }
    /* Register once: the scheduler resets parser state between polls but
     * keeps this callback, so every poll keeps delivering. */
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, on_row, NULL);

    printf("FIRMS monitor: polling %s every %lds (Ctrl+C to stop)\n",
           url, interval);

    if (snap_ctx_start(ctx) != SNAP_OK) {
        fprintf(stderr, "failed to start scheduler\n");
        snap_ctx_free(ctx);
        return 1;
    }

    /* The scheduler owns the timing; we only wait for shutdown. */
    while (!g_shutdown) sleep(1);

    printf("\nstopping...\n");
    snap_ctx_stop(ctx);
    snap_ctx_free(ctx);
    return 0;
}