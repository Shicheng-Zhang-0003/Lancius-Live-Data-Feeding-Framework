#ifndef PARSER_CSV_H
#define PARSER_CSV_H

#include "snapshot.h"

typedef void (*csv_row_cb_t)(const char **fields, int n_fields, void *udata);

void snap_csv_set_callback(void *parser_ctx, csv_row_cb_t cb, void *udata);

#endif