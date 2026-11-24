//
// Created by Kirill Ostrovnoy on 15.10.2026.
//

#include "postgres.h"

/* These are always necessary for a bgworker */

#include "miscadmin.h"

#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
/* these headers are used by this particular worker's code */
// #include "access/xact.h"
// #include "commands/dbcommands.h"
#include "executor/spi.h"
// #include "fmgr.h"
// #include "lib/stringinfo.h"
#include "storage/lwlock.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"
// #include "utils/snapmgr.h"
#include "dwen_global.h"
#include "dwen_light.h"
#include "utils/memutils.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(get_engine_status_active);
PG_FUNCTION_INFO_V1(set_engine_status_active);
PG_FUNCTION_INFO_V1(dwen_resume_engine);


int	max_dwen_workers = 1;
int dwen_active_execution_workers_count = 1;

char *dwen_database = NULL;
char *dwen_role = NULL;


DWEnWorkers *WorkersArr;
DWEnEngineStateType *DWEnEngineState;

// #define LOG_CONTEXT_LOCK_NAME  "log_context_lock"
// LWLock*      LogContextLWLock;

static void
dwen_shmem_request(void)
{
    Size mem_to_request = 0;

    if(prev_shmem_request_hook)
        prev_shmem_request_hook();

    mem_to_request = add_size(mem_to_request, MAXALIGN(sizeof(DWEnWorkers)));
    mem_to_request = add_size(mem_to_request, MAXALIGN(sizeof(DWEnEngineStateType)));

    RequestAddinShmemSpace(mem_to_request);
    RequestNamedLWLockTranche("DWEnWorkers", 1);
    RequestNamedLWLockTranche("DWEnEngineStateType", 1);
}

static void
dwen_shmem_startup(void)
{
    bool found;

    if(prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    WorkersArr = ShmemInitStruct("DWEnWorkers", sizeof(DWEnWorkers),
                                   &found);

    if(!found) {
        WorkersArr->lock = &(GetNamedLWLockTranche("DWEnWorkers"))->lock;
        WorkersArr->EngineActive = true;
    }

    DWEnEngineState = ShmemInitStruct("DWEnEngineStateType", sizeof(DWEnEngineStateType),
                                   &found);
    if(!found) {
        DWEnEngineState->lock = &(GetNamedLWLockTranche("DWEnEngineStateType"))->lock;
        DWEnEngineState->PrevMinutes = 0;
    }

    LWLockRelease(AddinShmemInitLock);
}


/*
 * Initialize workspace for a worker process: create the schema if it doesn't
 * already exist.
 */
// static void
// initialize_worker_spi(worktable *table)
// {
// 	int			ret;
// 	int			ntup;
// 	bool		isnull;
// 	StringInfoData buf;
//
// 	SetCurrentStatementStartTimestamp();
// 	StartTransactionCommand();
// 	SPI_connect();
// 	PushActiveSnapshot(GetTransactionSnapshot());
// 	pgstat_report_activity(STATE_RUNNING, "initializing worker_spi schema");
//
// 	/* XXX could we use CREATE SCHEMA IF NOT EXISTS? */
// 	initStringInfo(&buf);
// 	appendStringInfo(&buf, "select count(*) from pg_namespace where nspname
// = '%s'", 					 table->schema);
//
// 	debug_query_string = buf.data;
// 	ret = SPI_execute(buf.data, true, 0);
// 	if (ret != SPI_OK_SELECT)
// 		elog(FATAL, "SPI_execute failed: error code %d", ret);
//
// 	if (SPI_processed != 1)
// 		elog(FATAL, "not a singleton result");
//
// 	ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
// 									   SPI_tuptable->tupdesc,
// 									   1,
// &isnull)); 	if (isnull) 		elog(FATAL, "null result");
//
// 	if (ntup == 0)
// 	{
// 		debug_query_string = NULL;
// 		resetStringInfo(&buf);
// 		appendStringInfo(&buf,
// 						 "CREATE SCHEMA \"%s\" "
// 						 "CREATE TABLE \"%s\" ("
// 						 "		type text CHECK
// (type IN ('total', 'delta')), " 						 "		value	integer)" 						 "CREATE UNIQUE
// INDEX \"%s_unique_total\" ON \"%s\" (type) " 						 "WHERE type = 'total'",
// 						 table->schema, table->name,
// table->name, table->name);
//
// 		/* set statement start time */
// 		SetCurrentStatementStartTimestamp();
//
// 		debug_query_string = buf.data;
// 		ret = SPI_execute(buf.data, false, 0);
//
// 		if (ret != SPI_OK_UTILITY)
// 			elog(FATAL, "failed to create my schema");
//
// 		debug_query_string = NULL;	/* rest is not
// statement-specific */
// 	}
//
// 	SPI_finish();
// 	PopActiveSnapshot();
// 	CommitTransactionCommand();
// 	debug_query_string = NULL;
// 	pgstat_report_activity(STATE_IDLE, NULL);
// }



/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void _PG_init(void)
{
    BackgroundWorker eng_worker;

    if(!process_shared_preload_libraries_in_progress)
        elog(FATAL, "Please use shared_preload_librariesss");

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = dwen_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = dwen_shmem_startup;    
    
    DefineCustomStringVariable("dwen.database",
                                                       "Database to connect to.",
                                                       NULL,
                                                       &dwen_database,
                                                       "undefined_database",
                                                       PGC_SIGHUP,
                                                       0,
                                                       NULL, NULL, NULL);

    DefineCustomStringVariable("dwen.role",
                                                       "Role to connect with.",
                                                       NULL,
                                                       &dwen_role,
                                                       "undefined_role",
                                                       PGC_SIGHUP,
                                                       0,
                                                       NULL, NULL, NULL);

    DefineCustomIntVariable("dwen.max_execution_workers",
                            "Maximum active DWEn workers (parallel task executing)",
                            NULL,
                            &max_dwen_workers,
                            1,
                            1,
                            DWEN_MAX_WORKERS,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    dwen_active_execution_workers_count = max_dwen_workers;
    // elog(LOG, "DWEn _PG_init>>  max_dwen_workers = %d",max_dwen_workers);
    // ereport(NOTICE,
    //         (errmsg("DWEn _PG_init>>  max_dwen_workers = %d", max_dwen_workers)));
    /* get the configuration */

    /*
     * These GUCs are defined even if this library is not loaded with
     * shared_preload_libraries, for worker_spi_launch().

    DefineCustomIntVariable("worker_spi.naptime",
                                                    "Duration between each check
    (in seconds).", NULL, &worker_spi_naptime, 10, 1, INT_MAX, PGC_SIGHUP, 0,
                                                    NULL,
                                                    NULL,
                                                    NULL);


    */
    // Проверяем, что загрузка выполняется при старте сервера через
    // shared_preload_libraries

    ereport(WARNING,
            (errmsg("DWEn extension loaded")));
    elog(WARNING, "DWEn extension loaded");

    MarkGUCPrefixReserved("dwen");

    /* set up common data for all our workers */
    memset(&eng_worker, 0, sizeof(eng_worker));
    eng_worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    eng_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    eng_worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(eng_worker.bgw_library_name, "dwen_light");
    sprintf(eng_worker.bgw_function_name, "dwen_worker_engine");
    eng_worker.bgw_notify_pid = 0;

    /*
     * Now fill in worker-specific data, and do the actual registrations.
     *
     * bgw_extra can optionally include a database OID, a role OID and a set
     * of flags.  This is left empty here to fallback to the related GUCs at
     * startup (0 for the bgworker flags).
     */

    snprintf(eng_worker.bgw_name, BGW_MAXLEN, "pg_dwen engine worker");
    snprintf(eng_worker.bgw_type, BGW_MAXLEN, "pg_dwen");
    eng_worker.bgw_main_arg = Int32GetDatum(0);

    RegisterBackgroundWorker(&eng_worker);
    //EngineActive = true;
}


Datum
get_engine_status_active(PG_FUNCTION_ARGS)
{
    bool res;
    LWLockAcquire(WorkersArr->lock, LW_SHARED);
    res = WorkersArr->EngineActive;
    LWLockRelease(WorkersArr->lock);

    PG_RETURN_BOOL(res);
}

Datum
set_engine_status_active(PG_FUNCTION_ARGS)
{
    bool ns = PG_GETARG_BOOL(0);


    LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
    WorkersArr->EngineActive = ns;
    LWLockRelease(WorkersArr->lock);



    elog(LOG, "DWEN>> SET EngineActive to %s", ns ? "true" : "false"); // todo поправить логику ошибки
    PG_RETURN_BOOL(ns);

    //+ Воркеры висят в статусах ошибок и когда поправиликод процедуры и вызывают эту функцию - нужно воркеры тоже поднять
    //+ но нужно подумать и вмонтировать в логику когда воркер был прибит извне - поменять статус будет недостаточно
    // добавить логику сразу в dwen_resume_engine
}

Datum
dwen_resume_engine(PG_FUNCTION_ARGS)
{
    LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
    WorkersArr->EngineActive = true;
    LWLockRelease(WorkersArr->lock);
    elog(LOG, "DWEN>> Engine worker resumed");
    PG_RETURN_CSTRING("Engine worker resumed.");
}

// // Прочесываем пул исполнительных процессов
// for (int w_ind = 0; w_ind < dwen_active_execution_workers_count; w_ind++)
// {
//     // Получаем актуальную информацию по потоку
//     LWLockAcquire(WorkersArr->lock, LW_SHARED);
//     WorkerCurrentStatus = WorkersArr->Workers[w_ind].WorkerStatus;
//     TaskCurrentStatus = WorkersArr->Workers[w_ind].TaskStatus;
//     worker_task_id = WorkersArr->Workers[w_ind].TaskId;
//     worker_timeout_treshhold = WorkersArr->Workers[w_ind].TimeoutTreshhold;
//     worker_handle = WorkersArr->Workers[w_ind].pg_BackgroundWorkerHandle;
//     is_active = WorkersArr->EngineActive;
//     LWLockRelease(WorkersArr->lock);
//
//     /* Неблокирующая проверка текущего состояния */
//
//
//     // STOP HERE
//     // Проверка потока на ошибки
//     // if (WorkerCurrentStatus == DWEN_WORKER_STATUS_ACTIVE)
//     // {
//     //     status = GetBackgroundWorkerPid(worker_handle, &pid);
//     //     if (status == BGWH_STARTED) // Процесс живой
//     //     { /// А эту проверку возможно стоит делать для любого статуса воркера, ну кроме STOP - подумать
//     //
//     //     }
//     //
//     //     else if (status == BGWH_STOPPED) // был вызов kill -TERM
//     //         /// Вот это и нужно проуерить: воркер системой убивается - по идее его нужно запутсить - нужно пробовать
//     // }
//     if (WorkerCurrentStatus == DWEN_WORKER_STATUS_ERROR_SPI)
//     {
//     }
// }
