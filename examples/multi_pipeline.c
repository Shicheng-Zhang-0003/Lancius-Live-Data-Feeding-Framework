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
#include <signal.h>
#include <pthread.h>
#include <unistd.h>

static volatile sig_atomic_t g_shutdown = 0;

static void on_shutdown(int sig) { (void)sig; g_shutdown = 1; }

typedef struct {
    int fire_count;
    double max_frp;
    pthread_mutex_t lock;
} fire_stats_t;

static void fire_row_cb(const char **fields, int n_fields, void *udata) {
    fire_stats_t *stats = udata;
    if (!stats || n_fields < 12) return;

    char *end = NULL;
    double lat = strtod(fields[0], &end);
    if (end == fields[0]) return;
    double lon = strtod(fields[1], NULL);
    double frp = strtod(fields[11], NULL);
    int conf = atoi(fields[8]);

    if (conf >= 80 && frp > 50.0) {
        pthread_mutex_lock(&stats->lock);
        stats->fire_count++;
        if (frp > stats->max_frp) stats->max_frp = frp;
        pthread_mutex_unlock(&stats->lock);

        printf("FIRE %.4f, %.4f | FRP: %.1f | Conf: %d\n", lat, lon, frp, conf);
        fflush(stdout);
    }
}

static void climate_row_cb(const char **fields, int n_fields, void *udata) {
    (void)udata;
    if (n_fields < 8) return;
    /* Daymet single-pixel layout (after preamble skip): year,yday,dayl,prcp,srad,tmax,tmin,vp */
    double tmax = strtod(fields[5], NULL);
    double prcp = strtod(fields[3], NULL);
    if (tmax > 35.0 || prcp > 50.0) {
        printf("CLIMATE ALERT: Tmax=%.1fC Prcp=%.1fmm\n", tmax, prcp);
        fflush(stdout);
    }
}

static void *stats_printer(void *arg) {
    fire_stats_t *stats = arg;
    while (!g_shutdown) {
        for (int i = 0; i < 30 && !g_shutdown; i++) sleep(1);
        if (g_shutdown) break;
        pthread_mutex_lock(&stats->lock);
        printf("\nStats: %d high-conf fires, Max FRP: %.1f MW\n\n",
               stats->fire_count, stats->max_frp);
        fflush(stdout);
        pthread_mutex_unlock(&stats->lock);
    }
    return NULL;
}

int main(void) {
    signal(SIGINT, on_shutdown);
    signal(SIGTERM, on_shutdown);

    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { fprintf(stderr, "out of memory\n"); return 1; }
    fire_stats_t stats = {0};
    pthread_mutex_init(&stats.lock, NULL);

    /* Pipeline 1: FIRMS MODIS 7-day fire data */
    snap_source_t fire_src = {
        .name = "FIRMS-MODIS-7d",
        .url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_USA_contiguous_and_Hawaii_7d.csv",
        .format = SNAP_FMT_CSV,
        .interval_sec = 1800,  /* 30 min */
    };
    if (snap_ctx_add_source(ctx, &fire_src) != SNAP_OK) {
        fprintf(stderr, "Failed to add fire source\n");
        snap_ctx_free(ctx);
        return 1;
    }
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, fire_row_cb, &stats);

    /* Pipeline 2: Daymet climate for a location (San Francisco demo).
     * Daymet CSVs carry a 6-line preamble: skip it via parser_config. */
    snap_source_t clim_src = {
        .name = "Daymet-SF",
        .url = "https://daymet.ornl.gov/single-pixel/api/data?lat=37.7749&lon=-122.4194&vars=tmax,tmin,prcp&start=2024-01-01&end=2024-12-31",
        .format = SNAP_FMT_CSV,
        .interval_sec = 86400,  /* Daily */
        .parser_config = "skip=6",
    };
    if (snap_ctx_add_source(ctx, &clim_src) != SNAP_OK) {
        fprintf(stderr, "Failed to add climate source\n");
        snap_ctx_free(ctx);
        return 1;
    }
    snap_csv_set_callback(ctx->pipelines[1].parser.ctx, climate_row_cb, NULL);

    printf("=== Multi-Source Snapshot Pipeline ===\n");
    for (int i = 0; i < ctx->n_pipelines; i++) {
        printf("  [%d] %s -> every %ds\n", i,
               ctx->pipelines[i].source->name,
               ctx->pipelines[i].source->interval_sec);
    }
    printf("\n");

    pthread_t stats_thread;
    if (pthread_create(&stats_thread, NULL, stats_printer, &stats) != 0) {
        fprintf(stderr, "Failed to start stats thread\n");
        snap_ctx_free(ctx);
        return 1;
    }

    /* Initial snapshot */
    if (snap_ctx_run_once(ctx) != SNAP_OK)
        fprintf(stderr, "warning: initial fetch failed\n");

    /* Main loop - in production use snap_ctx_start() for async */
    while (!g_shutdown) {
        for (int i = 0; i < 60 && !g_shutdown; i++) sleep(1);
        if (g_shutdown) break;
        if (snap_ctx_run_once(ctx) != SNAP_OK)
            fprintf(stderr, "warning: fetch failed\n");
    }

    g_shutdown = 1;
    pthread_join(stats_thread, NULL);
    pthread_mutex_destroy(&stats.lock);
    snap_ctx_free(ctx);
    return 0;
}
