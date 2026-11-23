//
// Created by creator on 23.10.2026.
//

#ifndef PG_DWF_DWEN_WORKER_H
#define PG_DWF_DWEN_WORKER_H
#include "postgres.h"

static uint32 dwen_worker_wait_event_main = 0;
void dwen_worker_executor(Datum main_arg);
extern char *dwen_database;
extern char *dwen_role;
#endif //PG_DWF_DWEN_WORKER_H
