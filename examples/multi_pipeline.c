/*
 * LDFD - Example: multi-source snapshot with stats
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
 * Two sources on independent intervals plus a stats thread.
 *
 * This example is also the regression case for the multi-source bug: the
 * old fetch loop cleared still_running after the FIRST completed transfer,
 * so the second source was abandoned mid-flight and its easy handle leaked
 * at cleanup. Both sources now complete, which tests/test_fetch.c asserts
 * against a real loopback server.
 */

#include "snapshot.h"
#include "parser_csv.h"

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t g_shutdown = 0;
static void on_signal(int sig) { (void)sig; g_shutdown = 1; }

typedef struct {
    long fire_count;
    long clim_count;
    double max_frp;
    pthread_mutex_t lock;
} stats_t;

static void *stats_printer(void *arg) {
    stats_t *s = arg;
    while (!g_shutdown) {
        for (int i = 0; i < 30 && !g_shutdown; i++) sleep(1);
        if (g_shutdown) break;
        pthread_mutex_lock(&s->lock);
        printf("\n[stats] %ld high-conf fires (max FRP %.1f MW), "
               "%ld climate alerts\n\n",
               s->fire_count, s->max_frp, s->clim_count);
        fflush(stdout);
        pthread_mutex_unlock(&s->lock);
    }
    return NULL;
}

/* MODIS C6.1: lat0 lon1 ... conf8 ... frp11 */
static void fire_row(const char **f, int n, void *udata) {
    stats_t *s = udata;
    if (!s || n < 13) return;
    char *end = NULL;
    double lat = strtod(f[0], &end);
    if (end == f[0]) return;
    double lon = strtod(f[1], NULL);
    double frp = strtod(f[11], NULL);
    int conf = atoi(f[8]);

    if (conf >= 80 && frp > 50.0) {
        pthread_mutex_lock(&s->lock);
        s->fire_count++;
        if (frp > s->max_frp) s->max_frp = frp;
        pthread_mutex_unlock(&s->lock);
        printf("FIRE %.4f,%.4f | FRP %.1f | conf %d\n", lat, lon, frp, conf);
        fflush(stdout);
    }
}

/* Daymet single-pixel: year,yday,dayl,prcp,srad,tmax,tmin,vp (skip=6) */
static void climate_row(const char **f, int n, void *udata) {
    stats_t *s = udata;
    if (!s || n < 8) return;
    double tmax = strtod(f[5], NULL);
    double prcp = strtod(f[3], NULL);
    if (tmax > 35.0 || prcp > 50.0) {
        pthread_mutex_lock(&s->lock);
        s->clim_count++;
        pthread_mutex_unlock(&s->lock);
        printf("CLIMATE ALERT: tmax %.1fC prcp %.1fmm\n", tmax, prcp);
        fflush(stdout);
    }
}

int main(void) {
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { fprintf(stderr, "out of memory\n"); return 1; }

    stats_t stats;
    memset(&stats, 0, sizeof stats);
    pthread_mutex_init(&stats.lock, NULL);

    snap_source_t fire;
    memset(&fire, 0, sizeof fire);
    fire.name = "FIRMS-MODIS-7d";
    fire.url = (char *)"https://firms.modaps.eosdis.nasa.gov/data/active_fire/"
                     "modis-c6.1/csv/MODIS_C6_1_USA_contiguous_and_Hawaii_7d.csv";
    fire.format = SNAP_FMT_CSV;
    fire.interval_sec = 1800;
    if (snap_ctx_add_source(ctx, &fire) != SNAP_OK) {
        fprintf(stderr, "failed to add fire source\n");
        goto fail;
    }
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, fire_row, &stats);

    /* Daymet carries a 6-line preamble; skip=6 drops it before the header. */
    snap_source_t clim;
    memset(&clim, 0, sizeof clim);
    clim.name = "Daymet-SF";
    clim.url = (char *)"https://daymet.ornl.gov/single-pixel/api/data"
                     "?lat=37.7749&lon=-122.4194&vars=tmax,tmin,prcp"
                     "&start=2024-01-01&end=2024-12-31";
    clim.format = SNAP_FMT_CSV;
    clim.interval_sec = 86400;
    clim.parser_config = "skip=6";
    if (snap_ctx_add_source(ctx, &clim) != SNAP_OK) {
        fprintf(stderr, "failed to add climate source\n");
        goto fail;
    }
    snap_csv_set_callback(ctx->pipelines[1].parser.ctx, climate_row, &stats);

    printf("=== multi-source pipeline ===\n");
    for (int i = 0; i < ctx->n_pipelines; i++) {
        printf("  [%d] %-16s every %ds\n", i,
               ctx->pipelines[i].source->name,
               ctx->pipelines[i].source->interval_sec);
    }
    printf("(Ctrl+C to stop)\n\n");

    pthread_t th;
    if (pthread_create(&th, NULL, stats_printer, &stats) != 0) {
        fprintf(stderr, "failed to start stats thread\n");
        goto fail;
    }

    if (snap_ctx_start(ctx) != SNAP_OK) {
        fprintf(stderr, "failed to start scheduler\n");
        pthread_join(th, NULL);
        goto fail;
    }

    while (!g_shutdown) sleep(1);

    g_shutdown = 1;
    snap_ctx_stop(ctx);
    pthread_join(th, NULL);
    pthread_mutex_destroy(&stats.lock);
    snap_ctx_free(ctx);
    return 0;

fail:
    pthread_mutex_destroy(&stats.lock);
    snap_ctx_free(ctx);
    return 1;
}