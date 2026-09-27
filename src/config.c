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

snap_source_t *snap_source_from_json(cJSON *obj) {
    snap_source_t *src = calloc(1, sizeof(snap_source_t));
    
    cJSON *name = cJSON_GetObjectItem(obj, "name");
    cJSON *url = cJSON_GetObjectItem(obj, "url");
    cJSON *auth = cJSON_GetObjectItem(obj, "auth_header");
    cJSON *fmt = cJSON_GetObjectItem(obj, "format");
    cJSON *interval = cJSON_GetObjectItem(obj, "interval_sec");
    cJSON *parser_cfg = cJSON_GetObjectItem(obj, "parser_config");
    cJSON *trans_cfg = cJSON_GetObjectItem(obj, "transform_config");
    cJSON *out_cfg = cJSON_GetObjectItem(obj, "output_config");
    
    src->name = name ? strdup(name->valuestring) : NULL;
    src->url = url ? strdup(url->valuestring) : NULL;
    src->auth_header = auth ? strdup(auth->valuestring) : NULL;
    src->interval_sec = interval ? interval->valueint : 0;
    src->parser_config = parser_cfg ? strdup(parser_cfg->valuestring) : NULL;
    src->transform_config = trans_cfg ? strdup(trans_cfg->valuestring) : NULL;
    src->output_config = out_cfg ? strdup(out_cfg->valuestring) : NULL;
    
    if (fmt) {
        if (strcmp(fmt->valuestring, "csv") == 0) src->format = SNAP_FMT_CSV;
        else if (strcmp(fmt->valuestring, "json") == 0) src->format = SNAP_FMT_JSON;
        else if (strcmp(fmt->valuestring, "ndjson") == 0) src->format = SNAP_FMT_NDJSON;
        else src->format = SNAP_FMT_CSV;
    }
    
    return src;
}

snap_error_t snap_ctx_load_config(snap_ctx_t *ctx, const char *config_file) {
    FILE *f = fopen(config_file, "r");
    if (!f) return SNAP_ERR_CONFIG;
    
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    data[len] = '\0';
    fclose(f);
    
    cJSON *root = cJSON_Parse(data);
    free(data);
    if (!root) return SNAP_ERR_PARSE;
    
    cJSON *sources = cJSON_GetObjectItem(root, "sources");
    if (cJSON_IsArray(sources)) {
        cJSON *src_obj;
        cJSON_ArrayForEach(src_obj, sources) {
            snap_source_t *src = snap_source_from_json(src_obj);
            if (src) snap_ctx_add_source(ctx, src);
        }
    }
    
    cJSON_Delete(root);
    return SNAP_OK;
}