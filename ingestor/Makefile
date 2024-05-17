# Refer src/makefiles/pgxs.mk in postgres source for details about flags
MODULE_big = ingestor
OBJS = ingestor.o registry.o 
EXTENSION = ingestor     # the extersion's name
DATA = ingestor--0.0.1.sql    # script file to install
#REGRESS = get_sum_test      # the test script file

# `pkg-config --list-all | grep -e parquet` will find if installed
PG_CFLAGS += `pkg-config --cflags parquet-glib`
PG_CFLAGS += `pkg-config --cflags arrow-glib`
PG_CFLAGS += `pkg-config --cflags gio-2.0`
# Run pkg-config commands below to understand how outputs differ
# TODO - PG_LIBS libraries are not passed with `-l`. Using PD_LFAGS
# for both library include path(-L) and library name(-l)
PG_LDFLAGS  += `pkg-config --libs-only-L     parquet-glib`
PG_LDFLAGS   += `pkg-config --libs-only-l parquet-glib`
PG_LDFLAGS  += `pkg-config --libs-only-L     arrow-glib`
PG_LDFLAGS   += `pkg-config --libs-only-l arrow-glib`
PG_LDFLAGS  += `pkg-config --libs-only-L     gio-2.0`
PG_LDFLAGS   += `pkg-config --libs-only-l gio-2.0`

# for posgres build
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
