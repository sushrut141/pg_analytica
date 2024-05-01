CC=gcc

PARQUET_INCLUDE_DIR=/opt/homebrew/Cellar/apache-arrow-glib/16.0.0/include
ARROW_GLIB_INCLUDE_DIR=/opt/homebrew/Cellar/apache-arrow-glib/16.0.0/include
GLIB_INCLUDE_DIR=/opt/homebrew/Cellar/glib/2.80.0/include/glib-2.0
GLIB_INCLUDE_DIR2=/opt/homebrew/Cellar/glib/2.80.0/include/glib-2.0/include

CFLAGS=-g -Wall -O2
# `pkg-config --list-all | grep -e parquet` will find if installed
CFLAGS += `pkg-config --cflags parquet-glib`
CFLAGS += `pkg-config --cflags arrow-glib`
CFLAGS += `pkg-config --cflags gio-2.0`
# Run pkg-config commands below to understand how outputs differ
LDFLAGS  += `pkg-config --libs-only-L     parquet-glib`
LDLIBS   += `pkg-config --libs-only-l parquet-glib`
LDFLAGS  += `pkg-config --libs-only-L     arrow-glib`
LDLIBS   += `pkg-config --libs-only-l arrow-glib`
LDFLAGS  += `pkg-config --libs-only-L     gio-2.0`
LDLIBS   += `pkg-config --libs-only-l gio-2.0`

all: main

main: main.c
	$(CC) $(CFLAGS) -o main main.c $(LDFLAGS) $(LDLIBS)