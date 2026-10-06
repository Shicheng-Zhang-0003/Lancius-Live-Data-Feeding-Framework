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
 * Self-contained section: own line buffering, header capture, and typed
 * conversion. Deliberately independent of the generic snap_csv parser
 * (which consumes header rows internally), so no core files are touched.
 * Depends on libc only.
 */

#define _POSIX_C_SOURCE 200809L

#include "assoc.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

/* ============================================================================
 * Small helpers (libc only)
 * ============================================================================ */

assoc_bbox_t assoc_phase1_bbox(void) {
    assoc_bbox_t b = { 49.0, 51.2, -114.5, -110.0 };
    return b;
}

int assoc_in_bbox(const assoc_bbox_t *bbox, double lat, double lon) {
    if (!bbox) return 1;
    return lat >= bbox->lat_min && lat <= bbox->lat_max &&
           lon >= bbox->lon_min && lon <= bbox->lon_max;
}

static void copy_token(char *dst, size_t cap, const char *src) {
    if (!dst || cap == 0) return;
    if (!src) { dst[0] = '\0'; return; }
    strncpy(dst, src, cap - 1);
    dst[cap - 1] = '\0';
}

/* Lowercase + cut at first space/'(' so "prcp (mm/day)" -> "prcp". */
static void norm_name(const char *src, char *dst, size_t cap) {
    size_t j = 0;
    for (size_t i = 0; src[i] && j + 1 < cap; i++) {
        char c = src[i];
        if (c == ' ' || c == '(') break;
        dst[j++] = (char)tolower((unsigned char)c);
    }
    dst[j] = '\0';
    while (j > 0 && !isalnum((unsigned char)dst[j - 1])) dst[--j] = '\0';
}

static int header_has(const char **names, int n, const char *want) {
    char w[64];
    norm_name(want, w, sizeof w);
    for (int i = 0; i < n; i++) {
        char h[64];
        norm_name(names[i], h, sizeof h);
        if (strcmp(h, w) == 0) return i;
    }
    return -1;
}

static double num_or(const char *s, double dflt) {
    if (!s || !*s) return dflt;
    char *end = NULL;
    double v = strtod(s, &end);
    if (end == s) return dflt;
    return v;
}

/* ============================================================================
 * Minimal streaming CSV core: line buffer + header capture + row dispatch.
 * Handles \n, \r\n, and quoted fields (embedded commas/newlines in quotes).
 * ============================================================================ */

#define ASSOC_MAX_COLS 64
#define ASSOC_MAX_LINE (1u << 20) /* 1MB sanity cap per line */

typedef struct assoc_core {
    char *line;
    size_t len, cap;
    int overflow;        /* current line exceeded ASSOC_MAX_LINE: reject it */
    int closed_quote;    /* saw a closing quote; next byte must be , or EOL */
    char *names[ASSOC_MAX_COLS];
    int n_names;
    int header_done;
    const char **known;
    int n_known;
    void (*row)(const char **fields, int n_fields,
                const char **names, int n_names, void *udata);
    void *udata;
} assoc_core_t;

static void core_push(assoc_core_t *g, char c) {
    if (g->len + 1 >= g->cap) {
        size_t nc = g->cap ? g->cap * 2 : 1024;
        if (nc > ASSOC_MAX_LINE) {
            /* Despot truth: mark the row poisoned instead of dropping bytes
             * silently. A truncated field parses as a valid-looking number,
             * so emitting it would fabricate measurements. */
            g->overflow = 1;
            return;
        }
        char *nd = realloc(g->line, nc);
        if (!nd) { g->overflow = 1; return; }
        g->line = nd;
        g->cap = nc;
    }
    g->line[g->len++] = c;
}

/* Does this token start with a number? Used to tell Daymet preamble lines
 * ("Latitude: 37.7749 ...") from data rows while no header is known yet. */
static int looks_numeric(const char *s) {
    if (!s) return 0;
    while (*s == ' ' || *s == '\t') s++;
    char *end = NULL;
    strtod(s, &end);
    return end && end != s;
}

/*
 * Split line into fields (comma-separated, RFC4180 double-quote aware, in
 * place). Surrounding quotes are stripped and "" collapses to a single
 * quote. Returns the field count, or -1 if the row must be rejected:
 * unterminated quote, junk after a closing quote, or more than `cap` fields.
 * Rejecting (rather than truncating) is deliberate: a silently shortened
 * row is indistinguishable from a real measurement.
 */
static int core_split(char *line, const char **out, int cap) {
    int n = 0;
    char *p = line;
    char *field = line;
    int in_q = 0;
    int closed = 0;   /* previous byte was a closing quote */

    while (*p) {
        char c = *p;
        if (in_q) {
            if (c == '"') {
                if (p[1] == '"') {
                    /* escaped quote: shift the tail left, then step over the
                     * single surviving quote (advance-after-shift matters:
                     * """" must decode to one quote, not empty). */
                    memmove(p, p + 1, strlen(p + 1) + 1);
                    p++;
                    continue;
                }
                *p = '\0';   /* closing quote ends the content */
                in_q = 0;
                closed = 1;
                p++;
                continue;
            }
            p++;
            continue;
        }
        if (closed) {
            /* RFC4180: only a delimiter or end-of-line may follow a quote. */
            if (c != ',') return -1;
            closed = 0;
        }
        if (c == '"' && p == field && !in_q) {
            in_q = 1;         /* opening quote is stripped by advancing field */
            field = p + 1;
            p++;
            continue;
        }
        if (c == ',') {
            *p = '\0';
            if (n >= cap) return -1;
            out[n++] = field;
            field = p + 1;
            p++;
            continue;
        }
        p++;
    }
    if (in_q) return -1;            /* unterminated quote */
    if (n >= cap) return -1;        /* more fields than we can hold */
    out[n++] = field;
    return n;
}

static void core_emit_line(assoc_core_t *g) {
    if (g->overflow) {
        /* Over-long or unrecoverable row: drop it whole. Emitting the
         * surviving prefix would yield a truncated field that still parses
         * as a plausible number. */
        g->len = 0;
        g->overflow = 0;
        return;
    }
    if (g->len == 0) return;
    g->line[g->len] = '\0';
    const char *fields[ASSOC_MAX_COLS];
    /* core_split writes NULs into g->line; fields point into it. */
    int n = core_split(g->line, fields, ASSOC_MAX_COLS);
    g->len = 0;
    if (n <= 0) return;             /* malformed row: reject, do not guess */
    if (!g->row) return;            /* no sink: parse into nothing, safely */

    if (!g->header_done) {
        int hits = 0;
        for (int i = 0; i < n; i++) {
            char h[64];
            norm_name(fields[i], h, sizeof h);
            for (int k = 0; k < g->n_known; k++) {
                char w[64];
                norm_name(g->known[k], w, sizeof w);
                if (strcmp(h, w) == 0) { hits++; break; }
            }
        }
        if (hits > 0) {
            for (int i = 0; i < g->n_names; i++) free(g->names[i]);
            g->n_names = n;
            for (int i = 0; i < n; i++) g->names[i] = strdup(fields[i]);
            g->header_done = 1;
            return;                 /* header row is never data */
        }
        /* No header in sight. Preamble lines are prose ("Latitude: ...");
         * data rows start with a number. Forward only the latter so the
         * documented positional fallback actually works headerlessly. */
        if (!looks_numeric(fields[0])) return;
    }
    g->row(fields, n, (const char **)g->names, g->n_names, g->udata);
}

static void core_bytes(assoc_core_t *g, const unsigned char *data, size_t len) {
    if (!g) return;
    for (size_t i = 0; i < len; i++) {
        char c = (char)data[i];
        if (c == '\r') continue;
        if (c == '\n') { core_emit_line(g); continue; }
        core_push(g, c);
    }
}

static void core_free(assoc_core_t *g) {
    if (!g) return;
    core_emit_line(g); /* flush trailing line without newline */
    for (int i = 0; i < g->n_names; i++) free(g->names[i]);
    free(g->line);
    g->line = NULL;
    g->cap = g->len = 0;
}

/* ============================================================================
 * FIRMS fires
 * ============================================================================ */

struct assoc_fire_feed {
    assoc_core_t core;
    assoc_fire_cb_t cb;
    void *udata;
    assoc_fire_opts_t opts;
};

static const char *fire_col(const char **f, int n, const char **names, int nn,
                            const char *want, int fallback) {
    int idx = header_has(names, nn, want);
    if (idx >= 0 && idx < n) return f[idx];
    if (fallback >= 0 && fallback < n) return f[fallback];
    return "";
}

/* MODIS C6.1 positional fallback: lat0 lon1 bright2 ... date5 sat7 conf8 ... frp11 */
static void fire_row(const char **f, int n, const char **names, int nn, void *udata) {
    struct assoc_fire_feed *s = udata;
    if (n < 3) return;
    assoc_fire_t r;
    memset(&r, 0, sizeof r);
    r.confidence_num = -1;
    r.confidence_class = -1;

    r.lat = num_or(fire_col(f, n, names, nn, "latitude", 0), 0.0);
    r.lon = num_or(fire_col(f, n, names, nn, "longitude", 1), 0.0);
    const char *b = fire_col(f, n, names, nn, "brightness", -1);
    if (!*b) b = fire_col(f, n, names, nn, "bright_ti4", 2);
    r.brightness = num_or(b, 0.0);
    r.frp = num_or(fire_col(f, n, names, nn, "frp", 11), 0.0);
    copy_token(r.acq_date, sizeof r.acq_date, fire_col(f, n, names, nn, "acq_date", 5));
    copy_token(r.satellite, sizeof r.satellite, fire_col(f, n, names, nn, "satellite", 7));
    copy_token(r.confidence_raw, sizeof r.confidence_raw,
               fire_col(f, n, names, nn, "confidence", 8));

    char *end = NULL;
    double v = strtod(r.confidence_raw, &end);
    if (end != r.confidence_raw && *end == '\0') {
        r.confidence_num = v;
    } else {
        char c[16];
        norm_name(r.confidence_raw, c, sizeof c);
        if (strcmp(c, "h") == 0 || strcmp(c, "high") == 0) r.confidence_class = 2;
        else if (strcmp(c, "n") == 0 || strcmp(c, "nominal") == 0) r.confidence_class = 1;
        else if (strcmp(c, "l") == 0 || strcmp(c, "low") == 0) r.confidence_class = 0;
    }

    if (s->opts.min_frp >= 0 && r.frp < s->opts.min_frp) return;
    if (!assoc_in_bbox(s->opts.bbox, r.lat, r.lon)) return;
    s->cb(&r, s->udata);
}

static const char *fire_known[] = { "latitude", "longitude", "brightness", "bright_ti4",
                                    "acq_date", "satellite", "confidence", "frp" };

assoc_fire_feed_t *assoc_fire_feed_new(assoc_fire_cb_t cb, void *udata,
                                       const assoc_fire_opts_t *opts) {
    if (!cb) return NULL;   /* despot truth: NULL cb was a guaranteed SIGSEGV
                             * on the first data row (core_emit_line called it
                             * unconditionally). Fail at construction instead. */
    struct assoc_fire_feed *s = calloc(1, sizeof *s);
    if (!s) return NULL;
    s->cb = cb;
    s->udata = udata;
    if (opts) s->opts = *opts;
    else { s->opts.min_frp = -1; s->opts.bbox = NULL; }
    s->core.known = fire_known;
    s->core.n_known = 8;
    s->core.row = fire_row;
    s->core.udata = s;
    return s;
}

void assoc_fire_feed(assoc_fire_feed_t *f, const unsigned char *data, size_t len) {
    if (f && data) core_bytes(&f->core, data, len);
}

void assoc_fire_feed_free(assoc_fire_feed_t *f) {
    if (!f) return;
    core_free(&f->core);
    free(f);
}

/* ============================================================================
 * Daymet climate
 * ============================================================================ */

struct assoc_climate_feed {
    assoc_core_t core;
    assoc_climate_cb_t cb;
    void *udata;
};

static void clim_row(const char **f, int n, const char **names, int nn, void *udata) {
    struct assoc_climate_feed *s = udata;
    if (n < 3) return;
    assoc_climate_t r;
    memset(&r, 0, sizeof r);
    int iy = header_has(names, nn, "year");
    int id = header_has(names, nn, "yday");
    int itmax = header_has(names, nn, "tmax");
    int itmin = header_has(names, nn, "tmin");
    int ipr = header_has(names, nn, "prcp");
    int isr = header_has(names, nn, "srad");
    int idl = header_has(names, nn, "dayl");
    int ivp = header_has(names, nn, "vp");
    int iswe = header_has(names, nn, "swe");
    /* Daymet single-pixel canonical order fallback: year0 yday1 dayl2 prcp3 srad4 tmax5 tmin6 vp7 */
    if (iy < 0) iy = 0;
    if (id < 0) id = 1;
    if (idl < 0) idl = 2;
    if (ipr < 0) ipr = 3;
    if (isr < 0) isr = 4;
    if (itmax < 0) itmax = 5;
    if (itmin < 0) itmin = 6;
    if (ivp < 0) ivp = 7;
#define PICK(i) ((i) >= 0 && (i) < n ? f[i] : "")
    r.year = (int)num_or(PICK(iy), 0);
    r.yday = (int)num_or(PICK(id), 0);
    r.dayl_s = num_or(PICK(idl), 0);
    r.prcp_mmd = num_or(PICK(ipr), 0);
    r.srad_wm2 = num_or(PICK(isr), 0);
    r.tmax_c = num_or(PICK(itmax), 0);
    r.tmin_c = num_or(PICK(itmin), 0);
    r.vp_pa = num_or(PICK(ivp), 0);
    if (iswe >= 0 && iswe < n && *f[iswe]) { r.has_swe = 1; r.swe_mm = num_or(f[iswe], 0); }
#undef PICK
    if (r.year <= 0) return;
    s->cb(&r, s->udata);
}

static const char *clim_known[] = { "year", "yday", "dayl", "prcp",
                                    "srad", "tmax", "tmin", "vp" };

assoc_climate_feed_t *assoc_climate_feed_new(assoc_climate_cb_t cb, void *udata) {
    if (!cb) return NULL;   /* see assoc_fire_feed_new */
    struct assoc_climate_feed *s = calloc(1, sizeof *s);
    if (!s) return NULL;
    s->cb = cb;
    s->udata = udata;
    s->core.known = clim_known;
    s->core.n_known = 8;
    s->core.row = clim_row;
    s->core.udata = s;
    return s;
}

void assoc_climate_feed(assoc_climate_feed_t *f, const unsigned char *data, size_t len) {
    if (f && data) core_bytes(&f->core, data, len);
}

void assoc_climate_feed_free(assoc_climate_feed_t *f) {
    if (!f) return;
    core_free(&f->core);
    free(f);
}

/* ============================================================================
 * GBIF paged JSON — dependency-free scanner for {"results":[{...},...]}
 * ============================================================================ */

static const char *skip_ws(const char *p, const char *end) {
    while (p < end && isspace((unsigned char)*p)) p++;
    return p;
}

/* Find "key" at object depth and return value span. Handles strings w/ escapes. */
static int json_string_eq(const char *p, const char *end, const char *key, size_t klen,
                          const char **vs, size_t *vlen, int *is_string) {
    /* p points at opening quote of a key */
    const char *k = p + 1;
    size_t i = 0;
    while (k < end && i < klen && *k == key[i]) { k++; i++; }
    if (i != klen || k >= end || *k != '"') return 0;
    k++;
    k = skip_ws(k, end);
    if (k >= end || *k != ':') return 0;
    k = skip_ws(k + 1, end);
    if (k >= end) return 0;
    if (*k == '"') {
        const char *s = k + 1;
        const char *q = s;
        while (q < end) {
            if (*q == '\\' && q + 1 < end) { q += 2; continue; }
            if (*q == '"') break;
            q++;
        }
        if (q >= end) return 0;
        *vs = s; *vlen = (size_t)(q - s); *is_string = 1;
        return 1;
    }
    /* number / true / false / null: bare token until , } ] or ws */
    const char *q = k;
    while (q < end && *q != ',' && *q != '}' && *q != ']' && !isspace((unsigned char)*q)) q++;
    *vs = k; *vlen = (size_t)(q - k); *is_string = 0;
    return 1;
}

static int obj_field(const char *o, const char *oend, const char *key,
                     const char **vs, size_t *vlen, int *is_string) {
    size_t klen = strlen(key);
    const char *p = o;
    int in_str = 0, esc = 0, depth = 0;
    while (p < oend) {
        char c = *p;
        if (in_str) {
            if (esc) esc = 0;
            else if (c == '\\') esc = 1;
            else if (c == '"') in_str = 0;
            p++;
            continue;
        }
        if (c == '"') {
            /* only match keys at depth 1 (direct members) */
            if (depth == 1) {
                const char *vs2; size_t vl2; int is2;
                if (json_string_eq(p, oend, key, klen, &vs2, &vl2, &is2)) {
                    *vs = vs2; *vlen = vl2; *is_string = is2;
                    return 1;
                }
            }
            in_str = 1;
            p++;
            continue;
        }
        if (c == '{' || c == '[') depth++;
        else if (c == '}' || c == ']') depth--;
        p++;
    }
    return 0;
}

static double field_num(const char *o, const char *oend, const char *key, int *found) {
    const char *vs; size_t vl; int is;
    if (!obj_field(o, oend, key, &vs, &vl, &is)) { *found = 0; return 0; }
    char buf[64];
    size_t n = vl < sizeof buf - 1 ? vl : sizeof buf - 1;
    memcpy(buf, vs, n);
    buf[n] = '\0';
    char *e = NULL;
    double v = strtod(buf, &e);
    *found = (e != buf);
    return v;
}

static void field_str(const char *o, const char *oend, const char *key, char *dst, size_t cap) {
    const char *vs; size_t vl; int is;
    dst[0] = '\0';
    if (!obj_field(o, oend, key, &vs, &vl, &is)) return;
    /* unescape \" and \\ minimally */
    size_t j = 0;
    for (size_t i = 0; i < vl && j + 1 < cap; i++) {
        if (is && vs[i] == '\\' && i + 1 < vl) {
            i++;
            dst[j++] = vs[i];
        } else {
            dst[j++] = vs[i];
        }
    }
    dst[j] = '\0';
}

/* Locate the top-level "results" key. Must skip occurrences inside string
 * values: a record whose value contains the literal text "results" used to
 * hijack the scan and fail the whole page. Walks the document tracking string
 * state and object depth, and only accepts a key at depth 1 followed by ':'. */
static const char *find_results_key(const char *json, const char *end) {
    const char *needle = "\"results\"";
    const size_t nl = 9;
    const char *p = json;
    int depth = 0, in_str = 0, esc = 0;
    while (p + nl <= end) {
        char c = *p;
        if (in_str) {
            if (esc) esc = 0;
            else if (c == '\\') esc = 1;
            else if (c == '"') in_str = 0;
            p++;
            continue;
        }
        if (c == '"') {
            if (depth == 1 && memcmp(p, needle, nl) == 0) {
                /* Must be a KEY: a value that merely spells "results" is
                 * followed by , or } rather than a colon. */
                const char *q = skip_ws(p + nl, end);
                if (q < end && *q == ':') return p;
            }
            in_str = 1;
            p++;
            continue;
        }
        if (c == '{' || c == '[') depth++;
        else if (c == '}' || c == ']') depth--;
        p++;
    }
    return NULL;
}

int assoc_gbif_parse_page(const char *json, size_t len, const char *species_label,
                          assoc_occ_cb_t cb, void *udata,
                          const assoc_bbox_t *bbox, double max_uncert_m,
                          int *out_kept, int *out_skipped) {
    if (!json || len == 0 || !cb) return -1;
    const char *end = json + len;
    /* Declared before any goto: jumping over an initializer would leave the
     * tallies as garbage on the malformed path. */
    int kept = 0, skipped = 0;

    const char *rk = find_results_key(json, end);
    if (!rk) goto malformed;
    const char *arr = rk + 9;
    arr = skip_ws(arr, end);
    if (arr >= end || *arr != ':') goto malformed;
    arr = skip_ws(arr + 1, end);
    if (arr >= end || *arr != '[') goto malformed;

    const char *p = arr + 1;
    while (p < end) {
        p = skip_ws(p, end);
        if (p >= end) goto malformed;
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '{') goto malformed;
        /* match braces respecting strings */
        const char *o = p;
        int depth = 0, in_str = 0, esc = 0;
        const char *q = p;
        while (q < end) {
            char c = *q;
            if (in_str) {
                if (esc) esc = 0;
                else if (c == '\\') esc = 1;
                else if (c == '"') in_str = 0;
            } else {
                if (c == '"') in_str = 1;
                else if (c == '{') depth++;
                else if (c == '}') { depth--; if (depth == 0) break; }
            }
            q++;
        }
        if (q >= end) goto malformed;
        const char *oend = q + 1;

        int f1 = 0, f2 = 0, fu = 0;
        double lat = field_num(o, oend, "decimalLatitude", &f1);
        double lon = field_num(o, oend, "decimalLongitude", &f2);
        double unc = field_num(o, oend, "coordinateUncertaintyInMeters", &fu);
        if (!f1 || !f2) { skipped++; p = oend; continue; }
        if (fu && max_uncert_m >= 0 && unc > max_uncert_m) { skipped++; p = oend; continue; }
        if (!assoc_in_bbox(bbox, lat, lon)) { skipped++; p = oend; continue; }

        assoc_occ_t r;
        memset(&r, 0, sizeof r);
        copy_token(r.species, sizeof r.species, species_label ? species_label : "");
        r.lat = lat; r.lon = lon;
        r.uncert_m = fu ? unc : -1;
        field_str(o, oend, "eventDate", r.event_date, sizeof r.event_date);
        field_str(o, oend, "basisOfRecord", r.basis, sizeof r.basis);
        cb(&r, udata);
        kept++;
        p = oend;
    }
    if (out_kept) *out_kept = kept;
    if (out_skipped) *out_skipped = skipped;
    return kept;

malformed:
    /* Despot truth: a mid-document failure used to return -1 with the
     * out-params still zeroed, so a caller that had already received
     * records through cb saw "kept=0" for records it had been handed.
     * Report what actually happened and mark the document unusable. */
    if (out_kept) *out_kept = kept;
    if (out_skipped) *out_skipped = skipped;
    return -1;
}
