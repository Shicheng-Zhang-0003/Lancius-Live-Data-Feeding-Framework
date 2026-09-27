CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -O2 -Iinclude -fPIC
LDFLAGS = -lcurl -lcjson -lpthread

SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin

SOURCES = $(wildcard $(SRC_DIR)/*.c)
OBJECTS = $(SOURCES:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)

EXAMPLES = $(BIN_DIR)/fire_monitor $(BIN_DIR)/multi_pipeline

all: $(EXAMPLES)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
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

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR) libsnapshot.so

install: libsnapshot.so
	install -d /usr/local/include/snapshot
	install -m 644 include/snapshot.h /usr/local/include/snapshot/
	install -m 644 include/assoc.h /usr/local/include/snapshot/
	install -m 755 libsnapshot.so /usr/local/lib/

.PHONY: all clean install