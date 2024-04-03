MODULES = get_sum
EXTENSION = get_sum     # the extersion's name
DATA = get_sum--0.0.1.sql    # script file to install
#REGRESS = get_sum_test      # the test script file

# for posgres build
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
