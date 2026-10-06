/*
 * LDFD - Fetch / scheduler tests
 * Copyright (C) 2026
 *
 * Build (needs libcurl):
 *   gcc -std=c17 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude \
 *       tests/test_fetch.c src/fetch.c src/context.c src/buffer.c \
 *       src/parser_csv.c -lcurl -lpthread -o /tmp/test_fetch && /tmp/test_fetch
 *
 * Loopback-only: every transfer targets 127.0.0.1, so the suite never
 * touches the public internet. If no loopback listener can be started the
 * suite SKIPs rather than reporting a false green.
 */

#define _POSIX_C_SOURCE 200809L

#include "snapshot.h"
#include "parser_csv.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if !defined(__linux__)
int main(void) {
    printf("test_fetch: loopback harness is Linux-only; skipped\n");
    return 0;
}
#else

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>

static int failures;
static int checks;
static int skipped;

static void ok(int cond, const char *what) {
    checks++;
    printf("%s %s\n", cond ? "PASS" : "FAIL", what);
    if (!cond) failures++;
}

/* ---------------- tiny loopback HTTP/1.1 server ---------------- */

typedef struct {
    int listen_fd;
    int port;
    const char *body;
    int polls;          /* how many requests served */
    pthread_t th;
} server_t;

static void *server_thread(void *arg) {
    server_t *s = arg;
    for (;;) {
        int fd = accept(s->listen_fd, NULL, NULL);
        if (fd < 0) break;
        char req[2048];
        ssize_t n = recv(fd, req, sizeof req - 1, 0);
        (void)n;
        /* Drain request headers, then answer. */
        while (strstr(req, "\r\n\r\n") == NULL) {
            n = recv(fd, req + strlen(req), sizeof req - strlen(req) - 1, 0);
            if (n <= 0) break;
            req[strlen(req)] = '\0';
        }
        size_t blen = strlen(s->body);
        char head[256];
        int hn = snprintf(head, sizeof head,
                          "HTTP/1.1 200 OK\r\n"
                          "Content-Type: text/csv\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n\r\n", blen);
        ssize_t off = 0;
        while (off < hn) {
            ssize_t w = send(fd, head + off, (size_t)(hn - off), 0);
            if (w <= 0) break;
            off += w;
        }
        off = 0;
        while (off < (ssize_t)blen) {
            ssize_t w = send(fd, s->body + off, blen - (size_t)off, 0);
            if (w <= 0) break;
            off += w;
        }
        s->polls++;
        close(fd);
    }
    return NULL;
}

static int server_start(server_t *s, const char *body) {
    s->body = body;
    s->polls = 0;
    s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (s->listen_fd < 0) return 0;
    int one = 1;
    setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;                       /* ephemeral */
    if (bind(s->listen_fd, (struct sockaddr *)&a, sizeof a) != 0) return 0;
    if (listen(s->listen_fd, 8) != 0) return 0;
    socklen_t al = sizeof a;
    if (getsockname(s->listen_fd, (struct sockaddr *)&a, &al) != 0) return 0;
    s->port = ntohs(a.sin_port);
    if (pthread_create(&s->th, NULL, server_thread, s) != 0) return 0;
    return 1;
}

static void server_stop(server_t *s) {
    if (s->listen_fd >= 0) {
        shutdown(s->listen_fd, SHUT_RDWR);
        close(s->listen_fd);
        pthread_join(s->th, NULL);
        s->listen_fd = -1;
    }
}

static void url_for(char *buf, size_t n, int port, const char *path) {
    snprintf(buf, n, "http://127.0.0.1:%d%s", port, path);
}

/* ---------------- callback bookkeeping ---------------- */

static int      g_rows;
static int      g_header_as_data;
static int      g_last_first_is_data;

static void on_row(const char **f, int n, void *u) {
    (void)u;
    g_rows++;
    if (n <= 0) return;
    if (strcmp(f[0], "latitude") == 0) g_header_as_data++;
    g_last_first_is_data = (strcmp(f[0], "latitude") != 0);
}

/* ---------------- tests ---------------- */

/* A pure one-shot context must still terminate, or stop() would hang. */
static void test_oneshot_loop_terminates(void) {
    static const char body[] = "a,b\n1,2\n";
    server_t s;
    if (!server_start(&s, body)) {
        printf("SKIP one-shot termination (no loopback listener)\n");
        skipped++;
        return;
    }
    char url[128];
    url_for(url, sizeof url, s.port, "/term");

    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { ok(0, "term: ctx built"); server_stop(&s); return; }
    snap_source_t src = { "term", url, NULL, SNAP_FMT_CSV, 0,
                          NULL, NULL, NULL, NULL };
    snap_ctx_add_source(ctx, &src);

    /* snap_ctx_stop joins the loop thread. If the loop never exits, this
     * hangs, so the suite's own timeout is the assertion. */
    ok(snap_ctx_start(ctx) == SNAP_OK, "term: start");
    snap_ctx_stop(ctx);
    ok(1, "term: stop returned (loop thread exited, did not hang)");
    snap_ctx_free(ctx);
    server_stop(&s);
}

/* interval_sec was parsed, stored and printed, and read by nothing.
 *
 * Against real curl this caught a second, subtler bug: the loop exited as
 * soon as curl_multi_perform reported nothing in flight, but a periodic
 * context is idle for the entire gap between polls. So it fetched exactly
 * once and the loop was gone before the first deadline. The exit test now
 * also asks whether any deadline is still ahead. */
static void test_interval_reschedules(void) {
    static const char body[] = "latitude,longitude\n32.0,-81.0\n";
    server_t s;
    if (!server_start(&s, body)) {
        printf("SKIP interval scheduler (no loopback listener)\n");
        skipped++;
        return;
    }

    char url[128];
    url_for(url, sizeof url, s.port, "/poll");

    g_rows = 0; g_header_as_data = 0;
    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { ok(0, "interval: ctx built"); server_stop(&s); return; }

    /* interval 1s keeps the test quick */
    snap_source_t src = { "poll", url, NULL, SNAP_FMT_CSV, 1,
                          NULL, NULL, NULL, NULL };
    ok(snap_ctx_add_source(ctx, &src) == SNAP_OK, "interval: add_source");
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, on_row, NULL);

    ok(snap_ctx_start(ctx) == SNAP_OK, "interval: start");
    /* One initial fetch is immediate, then one per interval. Real timing
     * needs slack, so assert a lower bound rather than an exact count. */
    sleep(4);
    snap_ctx_stop(ctx);

    ok(g_rows >= 3, "interval: source re-polled several times");
    ok(s.polls >= 3, "interval: server saw repeated requests");
    ok(g_header_as_data == 0,
       "interval: header never re-emitted as data across polls");

    snap_ctx_free(ctx);
    server_stop(&s);
}

/* The old loop set still_running = 0 after the FIRST completion, so every
 * sibling source was abandoned mid-transfer and its handle leaked. */
static void test_multi_source_no_loss(void) {
    static const char body[] = "a,b\n1,2\n";
    server_t s;
    if (!server_start(&s, body)) {
        printf("SKIP multi-source (no loopback listener)\n");
        skipped++;
        return;
    }

    char u1[128], u2[128], u3[128];
    url_for(u1, sizeof u1, s.port, "/one");
    url_for(u2, sizeof u2, s.port, "/two");
    url_for(u3, sizeof u3, s.port, "/three");

    g_rows = 0;
    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { ok(0, "multi: ctx built"); server_stop(&s); return; }

    /* one-shot (interval 0) so all three must complete on the first pass */
    snap_source_t a = { "a", u1, NULL, SNAP_FMT_CSV, 0, NULL, NULL, NULL, NULL };
    snap_source_t b = { "b", u2, NULL, SNAP_FMT_CSV, 0, NULL, NULL, NULL, NULL };
    snap_source_t c = { "c", u3, NULL, SNAP_FMT_CSV, 0, NULL, NULL, NULL, NULL };
    snap_ctx_add_source(ctx, &a);
    snap_ctx_add_source(ctx, &b);
    snap_ctx_add_source(ctx, &c);
    for (int i = 0; i < ctx->n_pipelines; i++)
        snap_csv_set_callback(ctx->pipelines[i].parser.ctx, on_row, NULL);

    ok(snap_ctx_start(ctx) == SNAP_OK, "multi: start");
    sleep(2);
    snap_ctx_stop(ctx);

    ok(s.polls == 3, "multi: all three sources were fetched");
    ok(g_rows == 3, "multi: all three sources delivered rows");

    snap_ctx_free(ctx);
    server_stop(&s);
}

/* snap_ctx_run_once reuses the parser, which replayed the header row as
 * data on every call after the first. */
static void test_run_once_header_not_replayed(void) {
    static const char body[] = "latitude,longitude\n33.0,-82.0\n";
    server_t s;
    if (!server_start(&s, body)) {
        printf("SKIP run_once re-poll (no loopback listener)\n");
        skipped++;
        return;
    }

    char url[128];
    url_for(url, sizeof url, s.port, "/once");

    g_rows = 0; g_header_as_data = 0;
    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { ok(0, "run_once: ctx built"); server_stop(&s); return; }
    snap_source_t src = { "once", url, NULL, SNAP_FMT_CSV, 60,
                          NULL, NULL, NULL, NULL };
    snap_ctx_add_source(ctx, &src);
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, on_row, NULL);

    ok(snap_ctx_run_once(ctx) == SNAP_OK, "run_once: first call");
    ok(g_rows == 1, "run_once: first poll emits exactly 1 row");
    ok(snap_ctx_run_once(ctx) == SNAP_OK, "run_once: second call");
    ok(g_rows == 2, "run_once: second poll emits exactly 1 more row");
    ok(g_header_as_data == 0, "run_once: header not replayed as data");

    snap_ctx_free(ctx);
    server_stop(&s);
}

/* One-shot sources must let the async loop reach its own exit condition.
 * ctx_untrack_task used to NULL the slot without shrinking n_tasks, so the
 * loop's `n_tasks == 0` test could never fire and the scheduler spun forever
 * on a 250ms timer after finishing all its work. */
static void test_oneshot_loop_drains(void) {
    static const char body[] = "a,b\n1,2\n";
    server_t s;
    if (!server_start(&s, body)) {
        printf("SKIP one-shot drain (no loopback listener)\n");
        skipped++;
        return;
    }
    char url[128];
    url_for(url, sizeof url, s.port, "/oneshot");

    snap_ctx_t *ctx = snap_ctx_new();
    if (!ctx) { ok(0, "drain: ctx built"); server_stop(&s); return; }
    /* interval 0 => one-shot, nothing to reschedule */
    snap_source_t src = { "oneshot", url, NULL, SNAP_FMT_CSV, 0,
                          NULL, NULL, NULL, NULL };
    snap_ctx_add_source(ctx, &src);
    snap_csv_set_callback(ctx->pipelines[0].parser.ctx, on_row, NULL);

    ok(snap_ctx_start(ctx) == SNAP_OK, "drain: start");
    /* long enough for the transfer to complete and the task be released */
    sleep(2);
    ok(ctx->n_tasks == 0,
       "drain: task count returns to 0 so the loop can exit");
    ok(s.polls == 1, "drain: one-shot source fetched exactly once");

    snap_ctx_stop(ctx);
    snap_ctx_free(ctx);
    server_stop(&s);
}

/* Two live contexts used to mean curl_global_init twice but one cleanup per
 * free, tearing down libcurl global state under the survivor. */
static void test_two_contexts(void) {
    static const char body[] = "a,b\n1,2\n";
    server_t s;
    if (!server_start(&s, body)) {
        printf("SKIP two-context (no loopback listener)\n");
        skipped++;
        return;
    }
    char url[128];
    url_for(url, sizeof url, s.port, "/two");

    snap_ctx_t *a = snap_ctx_new();
    snap_ctx_t *b = snap_ctx_new();
    ok(a && b, "two-ctx: both allocated");

    snap_source_t sa = { "a", url, NULL, SNAP_FMT_CSV, 0, NULL, NULL, NULL, NULL };
    ok(snap_ctx_add_source(a, &sa) == SNAP_OK, "two-ctx: add to first");

    snap_ctx_free(b);            /* must not break a */
    g_rows = 0;
    snap_ctx_add_source(a, &sa);
    snap_csv_set_callback(a->pipelines[1].parser.ctx, on_row, NULL);
    ok(snap_ctx_run_once(a) == SNAP_OK,
       "two-ctx: survivor still fetches after sibling freed");
    ok(g_rows == 1, "two-ctx: survivor delivered its row");

    snap_ctx_free(a);
    server_stop(&s);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    printf("== fetch / scheduler suite (loopback only) ==\n");

    test_oneshot_loop_terminates();
    test_interval_reschedules();
    test_multi_source_no_loss();
    test_run_once_header_not_replayed();
    test_oneshot_loop_drains();
    test_two_contexts();

    printf("\n%d checks, %d failures", checks, failures);
    if (skipped) printf(", %d skipped", skipped);
    printf("\n");
    if (failures) {
        printf("FETCH SUITE FAILED\n");
        return 1;
    }
    printf("fetch: all pass%s\n", skipped ? " (with skips)" : "");
    return 0;
}

#endif /* __linux__ */