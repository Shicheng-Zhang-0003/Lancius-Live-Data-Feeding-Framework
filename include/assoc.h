#ifndef ASSOC_H
#define ASSOC_H

/*
 * LDFD - Associated-data module (Idea1 ecological feeds)
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
 * Specialised section for Idea1 "associated data" (PDF Sec 6-7):
 * FIRMS active-fire CSV, Daymet climate CSV, GBIF occurrence JSON.
 *
 * Design: self-contained streaming feeds with their own line buffer and
 * header-name lookup, so MODIS vs VIIRS layouts and the Daymet 6-line
 * preamble + "prcp (mm/day)" style headers just work without touching
 * the generic CSV parser, context, fetch, or buffer code.
 * GBIF paging is parsed dependency-free (no cJSON required).
 *
 * Column lookup is header-name based, so MODIS vs VIIRS layouts and the
 * Daymet 6-line preamble + "prcp (mm/day)" style headers just work.
 * Use assoc_phase1_bbox() to match configs/params.yaml
 * (49.0-51.2N, -114.5 to -110.0W).
 */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bounding box filter; NULL means "no spatial filter". */
typedef struct assoc_bbox {
    double lat_min, lat_max;
    double lon_min, lon_max;
} assoc_bbox_t;

/* Phase-1 study bbox (configs/params.yaml). */
assoc_bbox_t assoc_phase1_bbox(void);
int assoc_in_bbox(const assoc_bbox_t *bbox, double lat, double lon);

/* ---- FIRMS active fire (MODIS C6.1 NRT + VIIRS V2 NRT layouts) ---- */
typedef struct assoc_fire {
    double lat, lon;
    double brightness;
    double frp;                 /* MW */
    char acq_date[16];          /* YYYY-MM-DD */
    char satellite[16];         /* e.g. "T", "N", "Terra" */
    char confidence_raw[16];    /* raw token: "88", "h", "high", "nominal" ... */
    double confidence_num;      /* numeric value when token parses, else -1 */
    int confidence_class;       /* 0=low, 1=nominal/n, 2=high/h, -1=unknown/numeric */
} assoc_fire_t;

typedef void (*assoc_fire_cb_t)(const assoc_fire_t *fire, void *udata);

typedef struct assoc_fire_opts {
    double min_frp;             /* drop rows with frp < min_frp; <0 disables */
    const assoc_bbox_t *bbox;   /* NULL disables */
} assoc_fire_opts_t;

/*
 * Self-contained streaming feed (own line buffer + header capture).
 * Feed raw HTTP chunks; header row is captured, Daymet-style preamble
 * rows are skipped, typed records go to cb. No dependency on the
 * generic snap_csv parser (which consumes headers internally).
 */
typedef struct assoc_fire_feed assoc_fire_feed_t;

assoc_fire_feed_t *assoc_fire_feed_new(assoc_fire_cb_t cb, void *udata,
                                       const assoc_fire_opts_t *opts);
void assoc_fire_feed(assoc_fire_feed_t *f, const unsigned char *data, size_t len);
void assoc_fire_feed_free(assoc_fire_feed_t *f);

/* ---- Daymet single-pixel CSV (6 preamble lines + header row) ---- */
typedef struct assoc_climate {
    int year, yday;
    double dayl_s;
    double prcp_mmd;            /* mm/day */
    double srad_wm2;
    double tmax_c, tmin_c;
    double vp_pa;
    int has_swe;
    double swe_mm;
} assoc_climate_t;

typedef void (*assoc_climate_cb_t)(const assoc_climate_t *clim, void *udata);

/* Self-contained streaming feed, same pattern as assoc_fire_feed_t. */
typedef struct assoc_climate_feed assoc_climate_feed_t;

assoc_climate_feed_t *assoc_climate_feed_new(assoc_climate_cb_t cb, void *udata);
void assoc_climate_feed(assoc_climate_feed_t *f, const unsigned char *data, size_t len);
void assoc_climate_feed_free(assoc_climate_feed_t *f);

/* ---- GBIF occurrence page (paged {"results":[...]} JSON) ---- */
typedef struct assoc_occ {
    char species[64];           /* caller-supplied label for the page query */
    double lat, lon;
    double uncert_m;            /* -1 when absent */
    char event_date[32];
    char basis[32];             /* e.g. HUMAN_OBSERVATION, PRESERVED_SPECIMEN */
} assoc_occ_t;

typedef void (*assoc_occ_cb_t)(const assoc_occ_t *occ, void *udata);

/*
 * Parse one GBIF search page. Dependency-free (no cJSON).
 * species_label: e.g. "Bromus tectorum" for this page.
 * bbox: NULL disables spatial filter. max_uncert_m: <0 disables uncertainty filter;
 *   records WITHOUT uncertainty are always kept (PDF Sec 10: down-weight, not drop).
 * Returns emitted count, or -1 on malformed JSON. Tallies via out_kept/out_skipped (nullable).
 */
int assoc_gbif_parse_page(const char *json, size_t len, const char *species_label,
                          assoc_occ_cb_t cb, void *udata,
                          const assoc_bbox_t *bbox, double max_uncert_m,
                          int *out_kept, int *out_skipped);

#ifdef __cplusplus
}
#endif

#endif /* ASSOC_H */
