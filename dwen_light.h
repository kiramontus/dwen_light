/*  */
/*  Created by Kirill Ostrovnoy on 23.10.2026. */
/*  */

#ifndef PG_DWF_PG_DWEN_H
#define PG_DWF_PG_DWEN_H

#include "miscadmin.h"
#include "storage/ipc.h"

static int	worker_spi_total_workers = 2;


/*  Shared memory */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* int	max_dwen_workers; */

/*  Datum get_engine_status_active(PG_FUNCTION_ARGS); */
/*  */
/*  Datum set_engine_status_active(PG_FUNCTION_ARGS); */
#endif //PPG_DWF_PG_DWEN_H