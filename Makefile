# Имя итоговой разделяемой библиотеки (.so), которая собирается из нескольких .c файлов
MODULE_big   = dwen_light

# Список объектных файлов (.o), полученных из ваших .c файлов
OBJS         = dwen_global.o dwen_light.o dwen_worker.o dwen_engine.o


EXTENSION = dwen_light
DATA = dwen_light--0.2.sql
PGFILEDESC = "dwen_light - DataWarehouse Engine Light"

#TAP_TESTS = 1
###
#top_builddir = /home/creator/CProjects
#include $(top_builddir)/src/Makefile.global
#include $(top_srcdir)/contrib/contrib-global.mk
###

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = /home/creator/CProjects
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif