CC ?= gcc
STD = c17
WARN = -Wall -Wextra -Wpedantic -Wshadow
DEFS = -D_POSIX_C_SOURCE=200809L
OPT = -O2

# Modern dependency handling: prefer pkg-config so multiarch include paths
# (/usr/include/x86_64-linux-gnu, /usr/include/cjson) resolve portably.
# Each module is queried separately on purpose: one `pkg-config --libs a b c`
# fails wholesale if any single module is unknown, which would silently drop
# the multiarch paths for the others too.
#
# zlib is required by src/decompress.c (gzip). It must be in LDFLAGS, not
# just on the machine: libz.so.1 is present at runtime, so the build gets all
# the way to the link and then fails with "DSO missing from command line".
PKG_CFLAGS := $(shell pkg-config --cflags libcurl 2>/dev/null) \
              $(shell pkg-config --cflags libcjson 2>/dev/null) \
              $(shell pkg-config --cflags zlib 2>/dev/null)
PKG_LIBS := $(shell pkg-config --libs libcurl 2>/dev/null || echo -lcurl) \
            $(shell pkg-config --libs libcjson 2>/dev/null || echo -lcjson) \
            $(shell pkg-config --libs zlib 2>/dev/null || echo -lz)

CFLAGS = -std=$(STD) $(DEFS) $(WARN) $(OPT) -Iinclude $(PKG_CFLAGS) -fPIC
LDFLAGS = $(PKG_LIBS) -lpthread

SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin

SOURCES = $(wildcard $(SRC_DIR)/*.c)
OBJECTS = $(SOURCES:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)

EXAMPLES = $(BIN_DIR)/fire_monitor $(BIN_DIR)/multi_pipeline $(BIN_DIR)/poll_once

all: $(EXAMPLES) libsnapshot.so

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c include/snapshot.h include/parser_csv.h \
                    include/assoc.h include/output.h
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR)/fire_monitor: $(OBJECTS) examples/fire_monitor.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) examples/fire_monitor.c $(OBJECTS) -o $@ $(LDFLAGS)

$(BIN_DIR)/multi_pipeline: $(OBJECTS) examples/multi_pipeline.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) examples/multi_pipeline.c $(OBJECTS) -o $@ $(LDFLAGS)

$(BIN_DIR)/poll_once: $(OBJECTS) examples/poll_once.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) examples/poll_once.c $(OBJECTS) -o $@ $(LDFLAGS)

libsnapshot.so: $(OBJECTS)
	$(CC) -shared $(OBJECTS) -o $@ $(LDFLAGS)

# ---------------------------------------------------------------- tests
#
# Two tiers:
#   make test         dependency-free suites (libc only) -- always runs
#   make test-fetch   fetch/scheduler suite (needs libcurl headers)
#
# The fetch tier is NOT in `test` because it needs libcurl development
# headers. It is reported as SKIP, never as a silent pass.

# zlib is optional-but-common; the decompression suite needs it.
HAVE_ZLIB := $(shell printf '#include <zlib.h>\nint main(void){return 0;}\n' \
              | $(CC) -x c - -fsyntax-only >/dev/null 2>&1 && echo yes)

ASSOC_TEST_BINS = /tmp/test_assoc /tmp/test_assoc_regress /tmp/test_core

test: $(ASSOC_TEST_BINS)
	@echo "--- assoc scenario suite ---"
	@/tmp/test_assoc
	@echo "--- assoc regression suite ---"
	@/tmp/test_assoc_regress
	@echo "--- core framework suite ---"
	@/tmp/test_core
	@if [ "$(HAVE_ZLIB)" = yes ]; then \
	    $(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Werror -Iinclude \
	        tests/test_decompress.c src/decompress.c src/buffer.c -lz \
	        -o /tmp/test_decompress && \
	    echo "--- decompression suite ---" && /tmp/test_decompress; \
	 else \
	    echo "SKIP decompression suite: zlib.h not found (install zlib1g-dev)."; \
	 fi

/tmp/test_assoc: tests/test_assoc.c src/assoc.c include/assoc.h
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Werror -Iinclude \
	    tests/test_assoc.c src/assoc.c -o $@

/tmp/test_assoc_regress: tests/test_assoc_regress.c src/assoc.c include/assoc.h
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Werror -Iinclude \
	    tests/test_assoc_regress.c src/assoc.c -o $@

/tmp/test_core: tests/test_core.c src/buffer.c src/parser_csv.c src/output.c \
                include/snapshot.h include/parser_csv.h include/output.h
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Werror -Iinclude \
	    tests/test_core.c src/buffer.c src/parser_csv.c src/output.c -o $@

# Fetch/scheduler tests need real libcurl headers: the option constants and
# struct layouts must match the linked library, so they are never stubbed.
HAVE_CURL := $(shell printf '#include <curl/curl.h>\nint main(void){return 0;}\n' \
              | $(CC) $(PKG_CFLAGS) -x c - -fsyntax-only >/dev/null 2>&1 && echo yes)
PKG_LIBS_CURL := $(shell pkg-config --libs libcurl 2>/dev/null || echo -lcurl)
# test-fetch compiles parser_json.c, so unlike the other suites it really does
# need to link cJSON, not just its headers.
PKG_LIBS_CJSON := $(shell pkg-config --libs libcjson 2>/dev/null || echo -lcjson)

# context.c resolves the CSV/JSON/NDJSON parser vtables, so the fetch suite
# transitively needs cJSON's headers even though it never calls them.
HAVE_CJSON := $(shell printf '#include <cjson/cJSON.h>\nint main(void){return 0;}\n' \
               | $(CC) $(PKG_CFLAGS) -x c - -fsyntax-only >/dev/null 2>&1 && echo yes)

test-fetch:
ifeq ($(HAVE_CURL),yes)
	@# Deliberately not $(LDFLAGS): this suite needs libcurl only. Linking
	@# cJSON here would make the fetch tests depend on libcjson-dev even
	@# though no cJSON symbol is used.
	@if [ "$(HAVE_CJSON)" = yes ]; then \
	    $(CC) $(CFLAGS) tests/test_fetch.c src/fetch.c src/context.c \
	        src/buffer.c src/parser_csv.c src/parser_json.c \
	        -o /tmp/test_fetch $(PKG_LIBS_CURL) $(PKG_LIBS_CJSON) -lpthread && \
	    /tmp/test_fetch; \
	 else \
	    echo "SKIP test-fetch: cJSON headers not found."; \
	    echo "      context.c resolves the JSON parser vtables, so this suite"; \
	    echo "      needs cJSON even though it never calls it."; \
	    echo "      install libcjson-dev (Debian/Ubuntu) or cjson-devel (Fedora)."; \
	    echo "      Run 'make test' for the dependency-free suites."; \
	 fi
else
	@echo "SKIP test-fetch: libcurl headers not found."
	@echo "      install libcurl4-openssl-dev (Debian/Ubuntu) or libcurl-devel (Fedora)."
	@echo "      Run 'make test' for the dependency-free suites."
endif

# Extra diagnostics (not part of the default build).
strict:
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Wpedantic -Wshadow -Wconversion \
	    -Werror=implicit-function-declaration -Iinclude $(PKG_CFLAGS) \
	    -fsyntax-only $(SOURCES) examples/*.c

analyze:
	$(CC) -std=$(STD) $(DEFS) -Wall -Wextra -Iinclude $(PKG_CFLAGS) \
	    -fanalyzer -fsyntax-only $(SOURCES)

# Sanitizers cover every dependency-free suite. It deliberately does NOT
# depend on `all`: building the examples needs libcurl + cJSON headers, so
# requiring them here would make `make sanitize` unusable on a machine that
# can still run the tests that matter.
sanitize: clean
	$(MAKE) CFLAGS="-std=$(STD) $(DEFS) -Wall -Wextra -Wpedantic -Wshadow -g -O1 \
	                -fno-omit-frame-pointer -Iinclude -fPIC \
	                -fsanitize=address,undefined" test

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR) libsnapshot.so \
	       /tmp/test_assoc /tmp/test_assoc_regress /tmp/test_core \
	       /tmp/test_decompress /tmp/test_fetch

install: libsnapshot.so
	install -d /usr/local/include/snapshot
	install -m 644 include/snapshot.h /usr/local/include/snapshot/
	install -m 644 include/assoc.h /usr/local/include/snapshot/
	install -m 644 include/parser_csv.h /usr/local/include/snapshot/
	install -m 755 libsnapshot.so /usr/local/lib/

.PHONY: all clean install test test-fetch strict analyze sanitize
