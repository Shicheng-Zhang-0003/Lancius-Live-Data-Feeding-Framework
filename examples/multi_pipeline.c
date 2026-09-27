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

static volatile int g_shutdown = 0;

typedef struct {
    int fire_count;
    double max_frp;
    pthread_mutex_t lock;
} fire_stats_t;

void fire_row_cb(const char **fields, int n_fields, void *udata) {
    fire_stats_t *stats = udata;
    if (n_fields < 12) return;
    
    double lat = atof(fields[0]);
    double lon = atof(fields[1]);
    double frp = atof(fields[11]);
    int conf = atoi(fields[8]);
    
    if (conf >= 80 && frp > 50.0) {
        pthread_mutex_lock(&stats->lock);
        stats->fire_count++;
        if (frp > stats->max_frp) stats->max_frp = frp;
        pthread_mutex_unlock(&stats->lock);
        
        printf("🔥 %.4f, %.4f | FRP: %.1f | Conf: %d%%\n", lat, lon, frp, conf);
    }
}

void climate_row_cb(const char **fields, int n_fields, void *udata) {
    if (n_fields < 8) return;
    double tmax = atof(fields[5]);
    double prcp = atof(fields[3]);
    if (tmax > 35.0 || prcp > 50.0) {
        printf("🌡️  ALERT: Tmax=%.1f°C Prcp=%.1fmm\n", tmax, prcp);
    }
}

void *stats_printer(void *arg) {
    fire_stats_t *stats = arg;
    while (!g_shutdown) {
        sleep(30);
        pthread_mutex_lock(&stats->lock);
        printf("\n📊 Stats: %d high-conf fires, Max FRP: %.1f MW\n\n",
               stats->fire_count, stats->max_frp);
        pthread_mutex_unlock(&stats->lock);
    }
    return NULL;
}

int main(void) {
    signal(SIGINT, (void*)1);  // Will break sleep
    
    snap_ctx_t *ctx = snap_ctx_new();
    fire_stats_t stats = {0};
    pthread_mutex_init(&stats.lock, NULL);
    
    /* Pipeline 1: FIRMS MODIS 7-day fire data */
    snap_source_t fire_src = {
        .name = "FIRMS-MODIS-7d",
        .url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_USA_contiguous_and_Hawaii_7d.csv",
        .format = SNAP_FMT_CSV,
        .interval_sec = 1800,  /* 30 min */
    };
    snap_ctx_add_source(ctx, &fire_src);
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, fire_row_cb, &stats);
    
    /* Pipeline 2: Daymet climate for a location (San Francisco) */
    snap_source_t clim_src = {
        .name = "Daymet-SF",
        .url = "https://daymet.ornl.gov/single-pixel/api/data?lat=37.7749&lon=-122.4194&vars=tmax,tmin,prcp&start=2024-01-01&end=2024-12-31",
        .format = SNAP_FMT_CSV,
        .interval_sec = 86400,  /* Daily */
    };
    snap_ctx_add_source(ctx, &clim_src);
    snap_csv_set_callback(ctx->pipelines[1].parser.ctx, climate_row_cb, NULL);
    
    printf("=== Multi-Source Snapshot Pipeline ===\n");
    for (int i = 0; i < ctx->n_pipelines; i++) {
        printf("  [%d] %s → every %ds\n", i, 
               ctx->pipelines[i].source->name,
               ctx->pipelines[i].source->interval_sec);
    }
    printf("\n");
    
    pthread_t stats_thread;
    pthread_create(&stats_thread, NULL, stats_printer, &stats);
    
    /* Initial snapshot */
    snap_ctx_run_once(ctx);
    
    /* Main loop - in production use snap_ctx_start() for async */
    while (!g_shutdown) {
        sleep(60);
        snap_ctx_run_once(ctx);
    }
    
    pthread_join(stats_thread, NULL);
    pthread_mutex_destroy(&stats.lock);
    snap_ctx_free(ctx);
    return 0;
}