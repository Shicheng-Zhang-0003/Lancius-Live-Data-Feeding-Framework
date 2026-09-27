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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static volatile int g_shutdown = 0;

void sig_handler(int sig) { g_shutdown = 1; }

/* Callback for each parsed CSV row */
void fire_row_cb(const char **fields, int n_fields, void *udata) {
    if (n_fields < 13) return;
    
    double lat = atof(fields[0]);
    double lon = atof(fields[1]);
    double frp = atof(fields[11]);
    const char *date = fields[5];
    const char *sat = fields[7];
    
    if (frp > 100.0) {  /* Only high-confidence, high-FRP fires */
        printf("🔥 FIRE: %.4f, %.4f | FRP: %.1f MW | %s | %s\n",
               lat, lon, frp, date, sat);
    }
}

int main(int argc, char **argv) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    
    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) return 1;
    
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
        return 1;
    }
    
    csv_parser_ctx_t *csv_ctx = ctx->pipelines[0].parser.ctx;
    csv_ctx->row_cb = fire_row_cb;
    
    printf("Starting fire monitor (Ctrl+C to stop)...\n");
    printf("Source: %s\n", src.name);
    printf("URL: %s\n", src.url);
    printf("Interval: %d seconds\n\n", src.interval_sec);
    
    snap_ctx_run_once(ctx);  /* Initial fetch */
    
    while (!g_shutdown) {
        sleep(src.interval_sec);
        if (g_shutdown) break;
        snap_ctx_run_once(ctx);
    }
    
    printf("\nShutting down...\n");
    snap_ctx_free(ctx);
    return 0;
}