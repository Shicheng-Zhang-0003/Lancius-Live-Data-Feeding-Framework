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
#include <curl/curl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    CURL *easy;
    char *url;
    char *auth_header;
    snap_parser_t *parser;
    void *parser_ctx;
    snap_buffer_t header_buf;
    int headers_done;
    snap_chunk_t chunk;
} fetch_task_t;

static size_t curl_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
    fetch_task_t *task = userdata;
    size_t total = size * nmemb;
    
    task->chunk.data = (uint8_t*)ptr;
    task->chunk.len = total;
    task->chunk.is_final = 0;
    
    if (task->parser && task->parser->feed) {
        task->parser->feed(task->parser_ctx, &task->chunk);
    }
    return total;
}

static size_t curl_header_cb(char *buffer, size_t size, size_t nitems, void *userdata) {
    fetch_task_t *task = userdata;
    size_t total = size * nitems;
    snap_buffer_append(&task->header_buf, (uint8_t*)buffer, total);
    return total;
}

static int curl_progress_cb(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
                             curl_off_t ultotal, curl_off_t ulnow) {
    return 0;
}

static fetch_task_t *fetch_task_new(const char *url, const char *auth,
                                     snap_parser_t *parser, void *parser_ctx) {
    fetch_task_t *t = calloc(1, sizeof(fetch_task_t));
    t->url = strdup(url);
    t->auth_header = auth ? strdup(auth) : NULL;
    t->parser = parser;
    t->parser_ctx = parser_ctx;
    snap_buffer_init(&t->header_buf, 1024);
    t->easy = curl_easy_init();
    
    curl_easy_setopt(t->easy, CURLOPT_URL, t->url);
    curl_easy_setopt(t->easy, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(t->easy, CURLOPT_WRITEDATA, t);
    curl_easy_setopt(t->easy, CURLOPT_HEADERFUNCTION, curl_header_cb);
    curl_easy_setopt(t->easy, CURLOPT_HEADERDATA, t);
    curl_easy_setopt(t->easy, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(t->easy, CURLOPT_PROGRESSFUNCTION, curl_progress_cb);
    curl_easy_setopt(t->easy, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(t->easy, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(t->easy, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(t->easy, CURLOPT_USERAGENT, "snapshot-framework/1.0");
    
    if (t->auth_header) {
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, t->auth_header);
        curl_easy_setopt(t->easy, CURLOPT_HTTPHEADER, headers);
    }
    
    return t;
}

static void fetch_task_free(fetch_task_t *t) {
    if (!t) return;
    curl_easy_cleanup(t->easy);
    free(t->url);
    free(t->auth_header);
    snap_buffer_free(&t->header_buf);
    free(t);
}

snap_error_t snap_fetch_once(const char *url, const char *auth_header,
                              snap_parser_t *parser, void *parser_ctx) {
    fetch_task_t *task = fetch_task_new(url, auth_header, parser, parser_ctx);
    
    CURLcode res = curl_easy_perform(task->easy);
    
    if (task->parser && task->parser->flush) {
        task->parser->flush(task->parser_ctx);
    }
    
    fetch_task_free(task);
    
    return res == CURLE_OK ? SNAP_OK : SNAP_ERR_CURL;
}

typedef struct {
    snap_ctx_t *ctx;
    CURLM *multi;
    int running;
} fetch_loop_t;

static void *fetch_loop(void *arg) {
    fetch_loop_t *loop = arg;
    int still_running = 1;
    
    while (!loop->ctx->shutdown && still_running) {
        CURLMcode mc = curl_multi_perform(loop->multi, &still_running);
        if (mc != CURLM_OK) break;
        
        if (still_running) {
            int numfds;
            mc = curl_multi_wait(loop->multi, NULL, 0, 1000, &numfds);
            if (mc != CURLM_OK) break;
        }
        
        CURLMsg *msg;
        int msgs_left;
        while ((msg = curl_multi_info_read(loop->multi, &msgs_left))) {
            if (msg->msg == CURLMSG_DONE) {
                fetch_task_t *task;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, &task);
                curl_multi_remove_handle(loop->multi, msg->easy_handle);
                
                if (task->parser && task->parser->flush) {
                    task->parser->flush(task->parser_ctx);
                }
                fetch_task_free(task);
            }
        }
    }
    return NULL;
}

snap_error_t snap_ctx_start(snap_ctx_t *ctx) {
    if (ctx->running) return SNAP_OK;
    
    ctx->multi_handle = curl_multi_init();
    ctx->shutdown = 0;
    
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        fetch_task_t *task = fetch_task_new(p->source->url, p->source->auth_header,
                                             &p->parser, NULL);
        curl_easy_setopt(task->easy, CURLOPT_PRIVATE, task);
        curl_multi_add_handle(ctx->multi_handle, task->easy);
    }
    
    fetch_loop_t *loop = malloc(sizeof(fetch_loop_t));
    loop->ctx = ctx;
    loop->multi = ctx->multi_handle;
    pthread_create(&ctx->pipelines[0].thread, NULL, fetch_loop, loop);
    
    ctx->running = 1;
    return SNAP_OK;
}

snap_error_t snap_ctx_stop(snap_ctx_t *ctx) {
    ctx->shutdown = 1;
    return SNAP_OK;
}