/*
 * LDFD - Associated-data module tests (no network, no extra deps).
 * Build: gcc -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude \
 *          tests/test_assoc.c src/assoc.c -o /tmp/test_assoc && /tmp/test_assoc
 */
#include "assoc.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static int fire_n;
static assoc_fire_t last_fire;

static void on_fire(const assoc_fire_t *f, void *udata) {
    (void)udata;
    fire_n++;
    last_fire = *f;
}

static int clim_n;
static assoc_climate_t last_clim;

static void on_clim(const assoc_climate_t *c, void *udata) {
    (void)udata;
    clim_n++;
    last_clim = *c;
}

static int occ_n;
static assoc_occ_t last_occ;

static void on_occ(const assoc_occ_t *o, void *udata) {
    (void)udata;
    occ_n++;
    last_occ = *o;
}

int main(void) {
    /* ---- FIRMS MODIS layout (matches fires/firms_ca_24h.csv header) ---- */
    {
        assoc_fire_opts_t opts = { -1, NULL };
        assoc_fire_feed_t *feed = assoc_fire_feed_new(on_fire, NULL, &opts);
        assert(feed);
        const char *text =
            "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n"
            "32.07066,-81.8846,302.66,1.71,1.28,2026-09-26,0209,T,51,6.1NRT,286.35,15.46,N\n";
        /* split chunks mid-row to prove streaming */
        assoc_fire_feed(feed, (const unsigned char *)text, 60);
        assoc_fire_feed(feed, (const unsigned char *)text + 60, strlen(text) - 60);
        assoc_fire_feed_free(feed);
        assert(fire_n == 1);
        assert(last_fire.lat > 32.07 && last_fire.lat < 32.08);
        assert(last_fire.frp > 15.4 && last_fire.frp < 15.5);
        assert(strcmp(last_fire.acq_date, "2026-09-26") == 0);
        assert(last_fire.confidence_num == 51.0);
    }
    /* ---- VIIRS confidence strings + FRP/bbox filter ---- */
    {
        assoc_bbox_t bb = assoc_phase1_bbox();
        assoc_fire_opts_t opts = { 10.0, &bb };
        assoc_fire_feed_t *feed = assoc_fire_feed_new(on_fire, NULL, &opts);
        assert(feed);
        fire_n = 0;
        const char *text =
            "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,confidence,version,bright_ti5,frp,daynight\n"
            "46.57526,-80.795,319.2,0.63,0.72,2026-09-26,0854,N,nominal,2.0NRT,280.09,2.72,N\n"
            "50.10000,-112.50000,338.55,0.62,0.72,2026-09-26,0854,N,high,2.0NRT,280.14,11.93,N";
        /* no trailing newline: free() must flush the last line */
        assoc_fire_feed(feed, (const unsigned char *)text, strlen(text));
        assoc_fire_feed_free(feed);
        /* row1: frp too low + outside bbox; row2: inside bbox, high conf */
        assert(fire_n == 1);
        assert(last_fire.confidence_class == 2);
        assert(strcmp(last_fire.confidence_raw, "high") == 0);
    }
    /* ---- Daymet preamble + spaced headers ---- */
    {
        assoc_climate_feed_t *feed = assoc_climate_feed_new(on_clim, NULL);
        assert(feed);
        const char *text =
            "Latitude: 37.7749  Longitude: -122.4194\n"
            "Tile: 11369\n"
            "year,yday,dayl (s),prcp (mm/day),srad (W/m^2),tmax (deg c),tmin (deg c),vp (Pa)\n"
            "2023,2,34011.34,9.53,120.25,11.81,6.74,983.53\n";
        assoc_climate_feed(feed, (const unsigned char *)text, strlen(text));
        assoc_climate_feed_free(feed);
        assert(clim_n == 1);
        assert(last_clim.year == 2023 && last_clim.yday == 2);
        assert(last_clim.tmax_c > 11.8 && last_clim.tmax_c < 11.82);
        assert(last_clim.prcp_mmd > 9.5 && last_clim.prcp_mmd < 9.54);
    }
    /* ---- GBIF page: bbox + uncertainty filters ---- */
    {
        const char *page =
            "{\"count\":3,\"results\":["
            "{\"decimalLatitude\":50.23,\"decimalLongitude\":-112.27,\"coordinateUncertaintyInMeters\":15.0,"
            "\"eventDate\":\"2026-04-15T10:07\",\"basisOfRecord\":\"HUMAN_OBSERVATION\"},"
            "{\"decimalLatitude\":42.67,\"decimalLongitude\":-80.33,\"coordinateUncertaintyInMeters\":4.0,"
            "\"eventDate\":\"2026-05-11\",\"basisOfRecord\":\"HUMAN_OBSERVATION\"},"
            "{\"decimalLatitude\":50.43,\"decimalLongitude\":-119.22,\"coordinateUncertaintyInMeters\":5000.0,"
            "\"eventDate\":\"2026-05-10\",\"basisOfRecord\":\"HUMAN_OBSERVATION\"}]}";
        assoc_bbox_t bb = assoc_phase1_bbox();
        int kept = 0, skipped = 0;
        occ_n = 0;
        int rc = assoc_gbif_parse_page(page, strlen(page), "Bromus tectorum",
                                       on_occ, NULL, &bb, 1000.0, &kept, &skipped);
        assert(rc == 1 && kept == 1 && skipped == 2 && occ_n == 1);
        assert(last_occ.lat > 50.2 && last_occ.lat < 50.3);
        assert(last_occ.uncert_m == 15.0);
        assert(strcmp(last_occ.species, "Bromus tectorum") == 0);
    }
    printf("assoc tests: all pass\n");
    return 0;
}
