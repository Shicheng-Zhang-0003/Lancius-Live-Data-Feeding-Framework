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
    struct curl_slist *req_headers; /* owned; freed with the task */
    snap_parser_t *parser;
    void *parser_ctx;
    snap_buffer_t header_buf;
    int headers_done;
    snap_chunk_t chunk;
    int pipeline;         /* owning pipeline index, or -1 for one-shot fetch */
    int persistent;       /* 1 if this source has interval_sec > 0 */
    /* Set only for snap_fetch_to_buffer(): accumulate the raw body here
     * instead of handing chunks to a parser. */
    snap_buffer_t *sink;
    size_t max_bytes;
    int overflow;         /* body exceeded max_bytes */
} fetch_task_t;

static size_t curl_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
    fetch_task_t *task = userdata;
    size_t total = size * nmemb;
    if (!task || total == 0) return 0;

    task->chunk.data = (uint8_t*)ptr;
    task->chunk.len = total;
    task->chunk.is_final = 0;

    if (task->sink) {
        if (task->max_bytes &&
            task->sink->len + total > task->max_bytes) {
            task->overflow = 1;
            return 0;   /* abort: a short body must not look like success */
        }
        snap_buffer_append(task->sink, (const uint8_t *)ptr, total);
        return total;
    }
    if (task->parser && task->parser->feed) {
        if (task->parser->feed(task->parser_ctx, &task->chunk) != SNAP_OK)
            return 0; /* abort transfer on parser error */
    }
    return total;
}

static size_t curl_header_cb(char *buffer, size_t size, size_t nitems, void *userdata) {
    fetch_task_t *task = userdata;
    size_t total = size * nitems;
    if (!task) return 0;
    snap_buffer_append(&task->header_buf, (uint8_t*)buffer, total);
    return total;
}

/* Modern xferinfo callback (CURLOPT_PROGRESSFUNCTION is deprecated). */
static int curl_xfer_cb(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
                        curl_off_t ultotal, curl_off_t ulnow) {
    (void)clientp; (void)dltotal; (void)dlnow; (void)ultotal; (void)ulnow;
    return 0;
}

static fetch_task_t *fetch_task_new(const char *url, const char *auth,
                                     snap_parser_t *parser, void *parser_ctx) {
    if (!url) return NULL;
    fetch_task_t *t = calloc(1, sizeof(fetch_task_t));
    if (!t) return NULL;
    t->url = strdup(url);
    if (!t->url) { free(t); return NULL; }
    t->auth_header = auth ? strdup(auth) : NULL;
    if (auth && !t->auth_header) { free(t->url); free(t); return NULL; }
    t->parser = parser;
    t->parser_ctx = parser_ctx;
    snap_buffer_init(&t->header_buf, 1024);
    t->easy = curl_easy_init();
    if (!t->easy) {
        free(t->url);
        free(t->auth_header);
        snap_buffer_free(&t->header_buf);
        free(t);
        return NULL;
    }

    t->pipeline = -1;
    t->persistent = 0;

    curl_easy_setopt(t->easy, CURLOPT_URL, t->url);
    curl_easy_setopt(t->easy, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(t->easy, CURLOPT_WRITEDATA, t);
    curl_easy_setopt(t->easy, CURLOPT_HEADERFUNCTION, curl_header_cb);
    curl_easy_setopt(t->easy, CURLOPT_HEADERDATA, t);
    curl_easy_setopt(t->easy, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(t->easy, CURLOPT_XFERINFOFUNCTION, curl_xfer_cb);
    curl_easy_setopt(t->easy, CURLOPT_XFERINFODATA, t);
    curl_easy_setopt(t->easy, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(t->easy, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(t->easy, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(t->easy, CURLOPT_USERAGENT, "snapshot-framework/1.0");

    /* Despot truth: an unauthenticated source must not be silently upgraded
     * to a credentialed one by a redirect. fetch_task_new() only attaches
     * the auth header when this task was built WITH one, and
     * CURLOPT_UNRESTRICTED_AUTH stays unset so libcurl itself drops the
     * Authorization header on a cross-host hop. Non-HTTP schemes stay
     * unreachable through a redirect, and TLS is actually verified. */
    /* curl >= 7.85 renamed these to *_STR; the LONG forms are deprecated and
     * slated for removal, so prefer the string variants when available. */
#if defined(LIBCURL_VERSION_NUM) && LIBCURL_VERSION_NUM >= 0x075500
    curl_easy_setopt(t->easy, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(t->easy, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
#else
    curl_easy_setopt(t->easy, CURLOPT_PROTOCOLS,
                     (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
    curl_easy_setopt(t->easy, CURLOPT_REDIR_PROTOCOLS,
                     (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
#endif
    curl_easy_setopt(t->easy, CURLOPT_UNRESTRICTED_AUTH, 0L);
    curl_easy_setopt(t->easy, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(t->easy, CURLOPT_SSL_VERIFYHOST, 2L);

    if (t->auth_header) {
        t->req_headers = curl_slist_append(NULL, t->auth_header);
        if (!t->req_headers) {
            curl_easy_cleanup(t->easy);
            free(t->url);
            free(t->auth_header);
            snap_buffer_free(&t->header_buf);
            free(t);
            return NULL;
        }
    }

    return t;
}

static int ctx_track_task(snap_ctx_t *ctx, fetch_task_t *t) {
    if (!ctx || !t) return 0;
    /* struct fetch_task* (header) vs fetch_task_t* (this file) are distinct
     * types at compile time; cast through void* at the boundary. */
    struct fetch_task **grown =
        realloc(ctx->tasks, (size_t)(ctx->n_tasks + 1) * sizeof(*ctx->tasks));
    if (!grown) return 0;
    ctx->tasks = grown;
    ctx->tasks[ctx->n_tasks++] = (struct fetch_task *)(void *)t;
    return 1;
}

static void ctx_untrack_task(snap_ctx_t *ctx, fetch_task_t *t) {
    if (!ctx || !ctx->tasks) return;
    for (int i = 0; i < ctx->n_tasks; i++) {
        if ((void *)ctx->tasks[i] != (void *)t) continue;
        /* Compact rather than leaving a NULL hole: the loop's exit test is
         * `n_tasks == 0`, so a NULLed slot that never shrinks the count would
         * keep the scheduler spinning after a one-shot source finished. */
        for (int j = i; j + 1 < ctx->n_tasks; j++) {
            ctx->tasks[j] = ctx->tasks[j + 1];
        }
        ctx->tasks[ctx->n_tasks - 1] = NULL;
        ctx->n_tasks--;
        return;
    }
}

static void fetch_task_free(fetch_task_t *t) {
    if (!t) return;
    if (t->easy) curl_easy_cleanup(t->easy);
    if (t->req_headers) curl_slist_free_all(t->req_headers);
    free(t->url);
    free(t->auth_header);
    snap_buffer_free(&t->header_buf);
    free(t);
}

/* Remove every task still tracked by ctx and free it. Used when the loop
 * exits early (shutdown or a curl_multi error) so no easy handle is left
 * attached to the multi handle at cleanup time. */
static void fetch_tasks_detach_all(snap_ctx_t *ctx) {
    if (!ctx || !ctx->multi_handle) return;
    for (int i = 0; i < ctx->n_tasks; i++) {
        fetch_task_t *t = (fetch_task_t *)(void *)ctx->tasks[i];
        if (!t) continue;
        if (t->easy) curl_multi_remove_handle((CURLM *)ctx->multi_handle, t->easy);
        ctx->tasks[i] = NULL;
        fetch_task_free(t);
    }
    ctx->n_tasks = 0;
}

snap_error_t snap_fetch_once(const char *url, const char *auth_header,
                              snap_parser_t *parser, void *parser_ctx) {
    if (!url) return SNAP_ERR_CONFIG;
    fetch_task_t *task = fetch_task_new(url, auth_header, parser, parser_ctx);
    if (!task) return SNAP_ERR_NOMEM;

    CURLcode res = curl_easy_perform(task->easy);

    if (task->parser && task->parser->flush) {
        task->parser->flush(task->parser_ctx);
    }

    fetch_task_free(task);

    return res == CURLE_OK ? SNAP_OK : SNAP_ERR_CURL;
}

/* One-shot fetch that appends the raw response body to `out`.
 *
 * This is the entry point the Python bridge uses (see ldfd_bridge.py): it
 * needs bytes, not parsed records, and doing that through a Python-registered
 * snap_parser_t would put a ctypes callback on the fetch hot path for no
 * benefit. `out` is caller-owned and reused across calls.
 *
 * max_bytes caps the accumulated size; 0 means no cap. Exceeding it is an
 * error rather than a silent truncation, because a truncated dataset that
 * looks complete is worse than a failed fetch. */
snap_error_t snap_fetch_to_buffer(const char *url, const char *auth_header,
                                  snap_buffer_t *out, size_t max_bytes) {
    if (!url || !out) return SNAP_ERR_CONFIG;

    fetch_task_t *task = fetch_task_new(url, auth_header, NULL, NULL);
    if (!task) return SNAP_ERR_NOMEM;

    task->sink = out;
    task->max_bytes = max_bytes;
    task->overflow = 0;

    CURLcode res = curl_easy_perform(task->easy);
    snap_error_t rc = SNAP_OK;

    if (task->overflow) rc = SNAP_ERR_OUTPUT;
    else if (res != CURLE_OK) rc = SNAP_ERR_CURL;

    /* Surface an HTTP error status: a 404 body is not a dataset, and
     * without this a saved error page looks like a successful fetch. */
    if (rc == SNAP_OK) {
        long code = 0;
        curl_easy_getinfo(task->easy, CURLINFO_RESPONSE_CODE, &code);
        if (code >= 400) rc = SNAP_ERR_CURL;
    }

    fetch_task_free(task);
    return rc;
}

typedef struct {
    snap_ctx_t *ctx;
    CURLM *multi;
} fetch_loop_t;

/* Monotonic seconds; immune to wall-clock jumps (NTP, DST, manual set). */
static double mono_now(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* Deadline for pipeline i: first poll is immediate, then every interval_sec.
 * interval_sec <= 0 means one-shot and is never rescheduled. */
static double sched_due_for(const snap_pipeline_t *p, double base) {
    if (!p || !p->source || p->source->interval_sec <= 0) return 0.0;
    return base + (double)p->source->interval_sec;
}

/* Is any periodic source still waiting for a future deadline?
 *
 * The loop's exit test cannot be "nothing in flight": a periodic context is
 * idle for the whole gap between polls, and treating that as finished made
 * interval_sec fire exactly once. curl_multi_perform legitimately reports 0
 * running between transfers, so without this the loop exited before the
 * first deadline ever arrived. */
static int sched_has_pending(const snap_ctx_t *ctx, double now) {
    if (!ctx || !ctx->next_due) return 0;
    for (int i = 0; i < ctx->n_pipelines; i++) {
        const snap_pipeline_t *p = &ctx->pipelines[i];
        if (!p || !p->source || p->source->interval_sec <= 0) continue;
        if (ctx->next_due[i] > now) return 1;   /* a deadline is still ahead */
    }
    return 0;
}

/* Arm any pipeline whose deadline has passed. Returns 1 if a task was
 * added, so the caller can keep the multi handle busy. */
static int sched_arm_due(snap_ctx_t *ctx, CURLM *multi, double now) {
    if (!ctx || !multi || !ctx->next_due) return 0;
    int armed = 0;
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        if (!p || !p->source || !p->source->url) continue;
        if (p->source->interval_sec <= 0) continue;   /* one-shot */
        if (ctx->next_due[i] == 0.0) continue;        /* already in flight */
        if (now < ctx->next_due[i]) continue;

        if (snap_pipeline_rearm(p) != SNAP_OK) {
            ctx->next_due[i] = 0.0;    /* stop trying this source */
            continue;
        }
        fetch_task_t *task = fetch_task_new(p->source->url, p->source->auth_header,
                                            &p->parser, p->parser.ctx);
        if (!task) { ctx->next_due[i] = 0.0; continue; }
        task->pipeline = i;
        task->persistent = 1;
        if (!ctx_track_task(ctx, task)) {
            fetch_task_free(task);
            ctx->next_due[i] = 0.0;
            continue;
        }
        curl_easy_setopt(task->easy, CURLOPT_PRIVATE, task);
        if (curl_multi_add_handle(multi, task->easy) != CURLM_OK) {
            ctx_untrack_task(ctx, task);
            fetch_task_free(task);
            ctx->next_due[i] = 0.0;
            continue;
        }
        ctx->next_due[i] = 0.0;          /* in flight */
        armed = 1;
    }
    return armed;
}

static void *fetch_loop(void *arg) {
    fetch_loop_t *loop = arg;
    /* Seed still_running=1 so the loop always runs curl_multi_wait at least
     * once: on an idle handle (everything one-shot and finished) perform()
     * reports 0, and exiting immediately would skip the scheduler entirely. */
    int still_running = 1;

    while (!loop->ctx->shutdown) {
        CURLMcode mc = curl_multi_perform(loop->multi, &still_running);
        if (mc != CURLM_OK) break;

        CURLMsg *msg;
        int msgs_left = 0;
        while ((msg = curl_multi_info_read(loop->multi, &msgs_left))) {
            if (msg->msg == CURLMSG_DONE) {
                fetch_task_t *task = NULL;
                curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, &task);
                curl_multi_remove_handle(loop->multi, msg->easy_handle);

                if (task) {
                    if (task->parser && task->parser->flush) {
                        task->parser->flush(task->parser_ctx);
                    }
                    /* Re-arm a recurring source. This is what interval_sec
                     * always claimed to do but never did: nothing read it.
                     * A fresh parser context per poll is required, otherwise
                     * CSV re-emits its header row as data on every re-poll. */
                    if (task->persistent && task->pipeline >= 0 &&
                        task->pipeline < loop->ctx->n_pipelines &&
                        loop->ctx->next_due) {
                        snap_pipeline_t *p = &loop->ctx->pipelines[task->pipeline];
                        snap_error_t rc = snap_pipeline_rearm(p);
                        loop->ctx->next_due[task->pipeline] =
                            (rc == SNAP_OK)
                                ? sched_due_for(p, mono_now())
                                : 0.0;   /* give up on a broken rearm */
                    }
                    ctx_untrack_task(loop->ctx, task);
                    fetch_task_free(task);
                }
                /* Despot truth: still_running is owned by curl_multi_perform
                 * and reflects ALL transfers. Forcing it to 0 here (the old
                 * "one-shot tasks" shortcut) tore down the loop after the
                 * FIRST completion, silently abandoning every sibling source
                 * mid-transfer and leaking its handle + task. Leave it to
                 * curl; the outer while re-checks it. */
            }
        }

        /* Re-arm anything whose interval has elapsed. Runs even when
         * still_running == 0, which is what keeps a purely periodic
         * context alive instead of exiting after the first poll. */
        double now = mono_now();
        int armed = sched_arm_due(loop->ctx, loop->multi, now);
        if (armed) still_running = 1;

        /* Nothing in flight, nothing armed, and no deadline ahead: done.
         * The `sched_has_pending` term is what lets a periodic context idle
         * between polls instead of exiting after the first one. */
        if (!still_running && loop->ctx->n_tasks == 0 &&
            !sched_has_pending(loop->ctx, now)) {
            break;
        }

        int numfds = 0;
        mc = curl_multi_wait(loop->multi, NULL, 0, 250, &numfds);
        if (mc != CURLM_OK) break;
    }

    /* Despot truth: on early exit (shutdown, or a curl_multi error) any
     * transfer still attached is stranded. curl_multi_cleanup refuses to
     * run while handles remain attached (CURLM_BAD_EASY_HANDLE) and then
     * leaks them, so detach and free each still-running task first. Tasks
     * are tracked in ctx->tasks precisely so this can be exhaustive. */
    fetch_tasks_detach_all(loop->ctx);
    free(loop);
    return NULL;
}

snap_error_t snap_ctx_start(snap_ctx_t *ctx) {
    if (!ctx) return SNAP_ERR_CONFIG;
    if (ctx->running) return SNAP_OK;
    if (ctx->n_pipelines == 0) return SNAP_ERR_CONFIG;

    ctx->multi_handle = curl_multi_init();  /* CURLM* */
    if (!ctx->multi_handle) return SNAP_ERR_CURL;
    ctx->shutdown = 0;

    /* Scheduler deadlines. All sources fire immediately on start; sources
     * with interval_sec > 0 are re-armed after each completion. */
    ctx->next_due = calloc((size_t)(ctx->n_pipelines > 0 ? ctx->n_pipelines : 1),
                           sizeof(double));
    if (!ctx->next_due) {
        snap_ctx_stop(ctx);
        return SNAP_ERR_NOMEM;
    }

    double now = mono_now();
    for (int i = 0; i < ctx->n_pipelines; i++) {
        snap_pipeline_t *p = &ctx->pipelines[i];
        if (!p->source || !p->source->url) continue;
        ctx->next_due[i] = now;              /* due immediately */
        p->running = 1;

        /* NOTE: pass the pipeline's live parser ctx so rows reach callbacks. */
        fetch_task_t *task = fetch_task_new(p->source->url, p->source->auth_header,
                                            &p->parser, p->parser.ctx);
        if (!task) {
            snap_ctx_stop(ctx);
            return SNAP_ERR_NOMEM;
        }
        task->pipeline = i;
        task->persistent = (p->source->interval_sec > 0);
        if (!ctx_track_task(ctx, task)) {
            fetch_task_free(task);
            snap_ctx_stop(ctx);
            return SNAP_ERR_NOMEM;
        }
        curl_easy_setopt(task->easy, CURLOPT_PRIVATE, task);
        CURLMcode mc = curl_multi_add_handle((CURLM *)ctx->multi_handle, task->easy);
        if (mc != CURLM_OK) {
            ctx_untrack_task(ctx, task);
            fetch_task_free(task);
            snap_ctx_stop(ctx);
            return SNAP_ERR_CURL;
        }
        /* next_due[i] stays 0 while in flight; the completion path re-arms it */
        ctx->next_due[i] = 0.0;
    }

    fetch_loop_t *loop = malloc(sizeof(fetch_loop_t));
    if (!loop) {
        snap_ctx_stop(ctx);
        return SNAP_ERR_NOMEM;
    }
    loop->ctx = ctx;
    loop->multi = (CURLM *)ctx->multi_handle;
    if (pthread_create(&ctx->loop_thread, NULL, fetch_loop, loop) != 0) {
        free(loop);
        snap_ctx_stop(ctx);
        return SNAP_ERR_CURL;
    }
    ctx->loop_thread_valid = 1;

    ctx->running = 1;
    return SNAP_OK;
}

snap_error_t snap_ctx_stop(snap_ctx_t *ctx) {
    if (!ctx) return SNAP_ERR_CONFIG;
    ctx->shutdown = 1;
    if (ctx->loop_thread_valid) {
        pthread_join(ctx->loop_thread, NULL);
        ctx->loop_thread_valid = 0;
    }
    if (ctx->multi_handle) {
        curl_multi_cleanup((CURLM *)ctx->multi_handle);
        ctx->multi_handle = NULL;
    }
    ctx->running = 0;
    return SNAP_OK;
}
