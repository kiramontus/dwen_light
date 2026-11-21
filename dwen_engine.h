//
// Created by creator on 24.10.2026.
//

#ifndef PG_DWF_DWEN_ENGINE_H
#define PG_DWF_DWEN_ENGINE_H

#include "postgres.h"


PGDLLEXPORT pg_noreturn void dwen_engine(Datum main_arg);
static uint32 dwen_engine_wait_event_main = 0;
extern int	max_dwen_workers;
extern int dwen_active_execution_workers_count;
extern char *dwen_database;
extern char *dwen_role;

bool DWEnSetNextTaskToWorker(int worker_index);
void task_set_status(uint32_t task_id, uint8_t task_status);
void StartDWEnExecutionWorker(int index);

#endif //PG_DWF_DWEN_ENGINE_H
