//
// Created by creator on 24.10.2026.
//

#ifndef PG_DWF_DWEN_ENGINE_H
#define PG_DWF_DWEN_ENGINE_H

#include "postgres.h"



static uint32 dwen_engine_wait_event_main = 0;
extern int	max_dwen_workers;
extern int dwen_active_execution_workers_count;
extern char *dwen_database;
extern char *dwen_role;

bool assign_next_task_to_worker(int worker_index);

#endif //PG_DWF_DWEN_ENGINE_H
