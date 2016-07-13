EXTENSION = pg_dbwa
DATA = sql/pg_dbwa--*.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
