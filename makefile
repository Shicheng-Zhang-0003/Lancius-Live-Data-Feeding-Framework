CC ?= gcc
STD = c17
WARN = -Wall -Wextra -Wpedantic -Wshadow
DEFS = -D_POSIX_C_SOURCE=200809L
OPT = -O2

# Modern dependency handling: prefer pkg-config so multiarch include paths
# (/usr/include/x86_64-linux-gnu, /usr/include/cjson) resolve portably.
# Falls back to -lcurl -lcjson when pkg-config is unavailable.
PKG_CFLAGS := $(shell pkg-config --cflags libcurl libcjson 2>/dev/null)
PKG_LIBS := $(shell pkg-config --libs libcurl libcjson 2>/dev/null)
ifeq ($(strip $(PKG_LIBS)),)
PKG_LIBS = -lcurl -lcjson
endif

CFLAGS = -std=$(STD) $(DEFS) $(WARN) $(OPT) -Iinclude $(PKG_CFLAGS) -fPIC
LDFLAGS = $(PKG_LIBS) -lpthread

SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin

SOURCES = $(wildcard $(SRC_DIR)/*.c)
OBJECTS = $(SOURCES:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)

EXAMPLES = $(BIN_DIR)/fire_monitor $(BIN_DIR)/multi_pipeline

all: $(EXAMPLES) libsnapshot.so

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c include/snapshot.h include/parser_csv.h include/assoc.h
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR)/fire_monitor: $(OBJECTS) examples/fire_monitor.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) examples/fire_monitor.c $(OBJECTS) -o $@ $(LDFLAGS)

$(BIN_DIR)/multi_pipeline: $(OBJECTS) examples/multi_pipeline.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) examples/multi_pipeline.c $(OBJECTS) -o $@ $(LDFLAGS)

libsnapshot.so: $(OBJECTS)
	$(CC) -shared $(OBJECTS) -o $@ $(LDFLAGS)

# Network-free unit tests (assoc module is libc-only; core needs curl/cjson).
test: /tmp/test_assoc
	/tmp/test_assoc

/tmp/test_assoc: tests/test_assoc.c src/assoc.c include/assoc.h
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Iinclude \
	    tests/test_assoc.c src/assoc.c -o /tmp/test_assoc && /tmp/test_assoc

# Extra diagnostics (not part of the default build).
strict:
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Wpedantic -Wshadow -Wconversion \
	    -Werror=implicit-function-declaration -Iinclude $(PKG_CFLAGS) \
	    -fsyntax-only $(SOURCES) examples/*.c

analyze:
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Iinclude $(PKG_CFLAGS) \
	    -fanalyzer -fsyntax-only $(SOURCES)

sanitize: clean
	$(MAKE) CFLAGS="$(CFLAGS) -g -fsanitize=address,undefined -fno-omit-frame-pointer" \
	        LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" all test

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR) libsnapshot.so /tmp/test_assoc

install: libsnapshot.so
	install -d /usr/local/include/snapshot
	install -m 644 include/snapshot.h /usr/local/include/snapshot/
	install -m 644 include/assoc.h /usr/local/include/snapshot/
	install -m 644 include/parser_csv.h /usr/local/include/snapshot/
	install -m 755 libsnapshot.so /usr/local/lib/

.PHONY: all clean install test strict analyze sanitize
