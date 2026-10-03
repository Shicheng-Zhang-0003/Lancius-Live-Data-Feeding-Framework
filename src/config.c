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
#include <stdio.h>
#include <cjson/cJSON.h>

static char *dup_str_or_null(const cJSON *item) {
    if (!cJSON_IsString(item) || !item->valuestring) return NULL;
    char *d = strdup(item->valuestring);
    return d;
}

static void snap_source_free_tmp(snap_source_t *src) {
    if (!src) return;
    free(src->name);
    free(src->url);
    free(src->auth_header);
    free(src->parser_config);
    free(src->transform_config);
    free(src->output_config);
    free(src);
}

snap_source_t *snap_source_from_json(struct cJSON *obj) {
    if (!cJSON_IsObject((cJSON *)obj)) return NULL;
    snap_source_t *src = calloc(1, sizeof(snap_source_t));
    if (!src) return NULL;

    cJSON *name = cJSON_GetObjectItem(obj, "name");
    cJSON *url = cJSON_GetObjectItem(obj, "url");
    cJSON *auth = cJSON_GetObjectItem(obj, "auth_header");
    cJSON *fmt = cJSON_GetObjectItem(obj, "format");
    cJSON *interval = cJSON_GetObjectItem(obj, "interval_sec");
    cJSON *parser_cfg = cJSON_GetObjectItem(obj, "parser_config");
    cJSON *trans_cfg = cJSON_GetObjectItem(obj, "transform_config");
    cJSON *out_cfg = cJSON_GetObjectItem(obj, "output_config");

    /* name + url are required; fail cleanly instead of inserting NULLs. */
    if (!cJSON_IsString(name) || !name->valuestring ||
        !cJSON_IsString(url) || !url->valuestring) {
        free(src);
        return NULL;
    }
    src->name = strdup(name->valuestring);
    src->url = strdup(url->valuestring);
    if (!src->name || !src->url) { snap_source_free_tmp(src); return NULL; }

    src->auth_header = dup_str_or_null(auth);
    src->parser_config = dup_str_or_null(parser_cfg);
    src->transform_config = dup_str_or_null(trans_cfg);
    src->output_config = dup_str_or_null(out_cfg);
    src->interval_sec = cJSON_IsNumber(interval) ? interval->valueint : 0;

    src->format = SNAP_FMT_CSV;
    if (cJSON_IsString(fmt) && fmt->valuestring) {
        if (strcmp(fmt->valuestring, "csv") == 0) src->format = SNAP_FMT_CSV;
        else if (strcmp(fmt->valuestring, "json") == 0) src->format = SNAP_FMT_JSON;
        else if (strcmp(fmt->valuestring, "ndjson") == 0) src->format = SNAP_FMT_NDJSON;
        else src->format = SNAP_FMT_CSV;
    }

    return src;
}

snap_error_t snap_ctx_load_config(snap_ctx_t *ctx, const char *config_file) {
    if (!ctx || !config_file) return SNAP_ERR_CONFIG;
    FILE *f = fopen(config_file, "r");
    if (!f) return SNAP_ERR_CONFIG;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return SNAP_ERR_CONFIG; }
    long len = ftell(f);
    if (len < 0 || len > (32 << 20)) { fclose(f); return SNAP_ERR_CONFIG; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return SNAP_ERR_CONFIG; }

    char *data = malloc((size_t)len + 1);
    if (!data) { fclose(f); return SNAP_ERR_NOMEM; }
    size_t got = fread(data, 1, (size_t)len, f);
    fclose(f);
    if ((long)got != len) { free(data); return SNAP_ERR_CONFIG; }
    data[len] = '\0';

    cJSON *root = cJSON_Parse(data);
    free(data);
    if (!root) return SNAP_ERR_PARSE;

    cJSON *sources = cJSON_GetObjectItem(root, "sources");
    if (!cJSON_IsArray(sources)) { cJSON_Delete(root); return SNAP_ERR_CONFIG; }
    cJSON *src_obj = NULL;
    cJSON_ArrayForEach(src_obj, sources) {
        snap_source_t *src = snap_source_from_json(src_obj);
        if (!src) continue; /* skip invalid entries, keep valid ones */
        snap_error_t rc = snap_ctx_add_source(ctx, src);
        /* add_source deep-copies; free the transient regardless. */
        snap_source_free_tmp(src);
        if (rc != SNAP_OK) { cJSON_Delete(root); return rc; }
    }

    cJSON_Delete(root);
    return SNAP_OK;
}
