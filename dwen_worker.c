//
// Created by creator on 23.10.2026.
//

#include "dwen_worker.h"
#include "dwen_global.h"

#include "postgres.h"

/* These are always necessary for a bgworker */

#include "miscadmin.h"

#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "storage/proc.h"

/* these headers are used by this particular worker's code */
// #include "access/xact.h"
// #include "commands/dbcommands.h"
#include "executor/spi.h"
// #include "fmgr.h"
// #include "lib/stringinfo.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"
// #include "utils/snapmgr.h"
#include "utils/memutils.h"


#include <stddef.h>

#include "../../CProjects/src/include/storage/procnumber.h"


PGDLLEXPORT pg_noreturn void dwen_worker(Datum main_arg);

static  MemoryContext extra_memory_context_worker = NULL;

void dwen_worker(Datum main_arg)
{
    int            index = DatumGetInt32(main_arg);
    char           log_msg[120];
    bits32         flags = 0;
    StringInfoData query;
    uint32_t       taskid;
    uint8_t        task_status;
    uint8_t        worker_status;

    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    pqsignal(SIGINT, StatementCancelHandler);

    BackgroundWorkerUnblockSignals();

    /* Connect to database */
    BackgroundWorkerInitializeConnection(dwen_database, dwen_role,  flags);

    initStringInfo(&query);
    int   ret;
    char  WaitEventExtension_txt[20];
    pid_t WorkerPID;

    //  todo Не всегда WorkersArr->Workers[index].WorkerProcNumber = MyProcNumber устанавливает номер процесса, иногда 0 получаается. разобраться почему
    // Установка статуса "готов к работе" в регистре потоков
    LWLockAcquire(WorkersArr->lock , LW_EXCLUSIVE);
    WorkersArr->Workers[index].WorkerStatus = worker_status  = DWEN_WORKER_STATUS_ACTIVE;
    WorkersArr->Workers[index].TaskStatus = task_status = DWEN_WORKER_TASK_STATUS_WAIT;
    WorkersArr->Workers[index].TaskId = 0;
    WorkersArr->Workers[index].WorkerProcNumber = MyProcNumber;
    WorkerPID = WorkersArr->Workers[index].pid;
    LWLockRelease(WorkersArr->lock );

    bool task_done_error = false;
    int wl_ret;

    sprintf(log_msg,"DWEn.%02d>> Execution worker index[%02d] started with PID %d", index, index, WorkerPID);
    elog(LOG, "%s", log_msg);
    dwen_log_event(DWEN_LOG_INFO, log_msg,0,"00000");

	for (;;)
	{
	    /* First time, allocate or get the custom wait event */
	    sprintf(WaitEventExtension_txt,"DWEn worker [%02d]",index);
	    if (dwen_worker_wait_event_main == 0)
	        dwen_worker_wait_event_main = WaitEventExtensionNew(WaitEventExtension_txt);

	    wl_ret = WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                         -1L,
                         dwen_worker_wait_event_main);
	    ResetLatch(MyLatch);

	    CHECK_FOR_INTERRUPTS();

	    /*
         * In case of a SIGHUP, just reload the configuration.
         */
	    if (ConfigReloadPending)
	    {
	        ConfigReloadPending = false;
	        ProcessConfigFile(PGC_SIGHUP);
	    }

	    LWLockAcquire(WorkersArr->lock , LW_EXCLUSIVE);
	    if (WorkersArr->Workers[index].TaskStatus == DWEN_WORKER_TASK_STATUS_NEW &&
	        WorkersArr->Workers[index].WorkerStatus == DWEN_WORKER_STATUS_ACTIVE )
	    {
	        taskid = WorkersArr->Workers[index].TaskId;
	        WorkersArr->Workers[index].TaskStatus = task_status = DWEN_WORKER_TASK_STATUS_EXECUTION;
	    }
	    else
	    {
	        taskid = 0;
	    }
	    LWLockRelease(WorkersArr->lock );

	    if (taskid > 0)
	    {
	        elog(LOG, "DWEn.%02d>> DEBUG Task to run: %d", index, taskid);

	        resetStringInfo(&query);
	        appendStringInfo(&query,
            "CALL dwen.task_execute_string(p_task_id := %d, error := NULL)",taskid);

	        MemoryContext work_context;
	        if (extra_memory_context_worker == NULL)
	            extra_memory_context_worker = AllocSetContextCreate(TopMemoryContext,
                                                               "DWEn engine worker log context",
                                                               ALLOCSET_DEFAULT_SIZES);

	        MemoryContextReset(extra_memory_context_worker);
	        work_context = MemoryContextSwitchTo(extra_memory_context_worker);

	        SetCurrentStatementStartTimestamp();
	        StartTransactionCommand();
	        SPI_connect();
	        PushActiveSnapshot(GetTransactionSnapshot());
	        debug_query_string = query.data;
	        pgstat_report_activity(STATE_RUNNING, query.data);

	        PG_TRY();
	        {
	            ret = SPI_execute(query.data, false, 0);
	            debug_query_string = NULL;

	            if (ret >0 ) // Нет ошибки SPI
	            {
	                if (SPI_processed  == 1)
	                {
	                    // Вернулась запись - dwen.task_execute_string вернул значение
	                    bool  isnull;
	                    task_done_error = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	                    if (!isnull)
	                    {
	                        if (!task_done_error)
	                            task_status = DWEN_WORKER_TASK_STATUS_DONE;
	                        else
	                            task_status = DWEN_WORKER_TASK_STATUS_ERROR; // Ошибка возникла при выполнении задачи
	                        // Ошибка перехвачена в dwen.task_execute_string в блок EXCEPTION
	                        // Ошибка SQL / plpgsql записывется в лог процедурой
	                    }
	                    else // Если запрос вернул строку, но там null - это поломанная функция dwen.task_execute_string
	                    {
	                        suspend_dwen_engine();
	                        task_done_error = true;
	                        task_status = DWEN_WORKER_TASK_STATUS_WARNING;
	                        worker_status = DWEN_WORKER_STATUS_INTERNAL_ERROR;
	                        ereport(LOG,
                                    errmsg("DWEn.%02d>> ERROR Execution worker has been suspended. dwen.task_execute_string return null." ,index)
                                    );
	                        ereport(LOG,
                                    errmsg("DWEn.%02d>> ERROR Task[%d] status will be set to 'WARNING'" ,index, taskid)
                                    );
	                        ereport(LOG,
                                    errmsg("DWEn.%02d>> ERROR The procedure dwen.task_execute_string was modified incorrectly.",index),
                                    errhint(
                                        "Restore the procedure code and then call procedure 'select dwen.set_engine_status_active(true)' to resume the process."
                                    ));
	                    }
	                }
	                else // Процедура dwen.task_execute_string не вернула значение или вернула больше одной строки
	                {
	                    suspend_dwen_engine();
	                    task_done_error = true;
	                    task_status = DWEN_WORKER_TASK_STATUS_WARNING;
	                    worker_status = DWEN_WORKER_STATUS_INTERNAL_ERROR;
	                    ereport(LOG,
                                errmsg("DWEn.%02d>> ERROR Execution worker has been suspended. dwen.task_execute_string return incorrect result." ,index)
                                );
	                    ereport(LOG,
	                            errmsg("DWEn.%02d>> ERROR Task[%d] status will be set to 'WARNING'" ,index, taskid)
                                );
	                    ereport(LOG,
                                errmsg("DWEn.%02d>> ERROR The procedure dwen.task_execute_string was modified incorrectly.",index),
                                errhint(
                                    "Restore the procedure code and then call procedure 'select dwen.set_engine_status_active(true)' to resume the process."
                                ));
	                }
	            }
	            else //  Ошибка SPI
	            {
	                suspend_dwen_engine();
	                ereport(LOG,
	                        errmsg("DWEn.%02d>> ERROR SPI: SPI_execute return %d. Engine worker has been suspended." ,index, ret)
                            );
	                task_status = DWEN_WORKER_TASK_STATUS_CANCELED; // Ошибка SPI - это 100% не выполненная транзакция
	                worker_status = DWEN_WORKER_STATUS_INTERNAL_ERROR;
	                // todo Добавить внутреннй тип ошибки в лог DWEN и передавать ее туда
	            }

	            SPI_finish();
	            PopActiveSnapshot();
	            CommitTransactionCommand();
	            debug_query_string = NULL;
	        }
	        PG_CATCH();
	        {
	            ErrorData* error_data;
	            MemoryContextSwitchTo(extra_memory_context_worker);
	            error_data = CopyErrorData();
	            FlushErrorState();
	            LWLockReleaseAll();
	            AbortCurrentTransaction();
	            MemoryContextSwitchTo(extra_memory_context_worker);

	            if (error_data->sqlerrcode == ERRCODE_QUERY_CANCELED) // Внешнее прерывание выполнения задачи
	            {
	                ereport(LOG,
                            errcode(error_data->sqlerrcode),
                            errmsg("DWEn.%02d>> Task %d canceled.", index, taskid)
                            );
	                task_status = DWEN_WORKER_TASK_STATUS_TIMEOUT;
	                worker_status = DWEN_WORKER_STATUS_ACTIVE; // todo Возможно стоит перечитывать
	            }
                else // другие ошибки
                {
                    suspend_dwen_engine();

                    debug_query_string = query.data;
                    ereport(LOG,
                            errcode(error_data->sqlerrcode),
                            errmsg("DWEn.%02d>> ERROR %s %s", index, unpack_sql_state(error_data->sqlerrcode), error_data->message));
                    debug_query_string = NULL;
                    ereport(LOG,
                            errmsg("DWEn.%02d>> ERROR Engine worker has been suspended.",index),
                            errhint(
                                "After the cause is resolved, call procedure 'select dwen.set_engine_status_active(true)' to resume the process."
                            ));
                    task_status = DWEN_WORKER_TASK_STATUS_CANCELED;
                    worker_status = DWEN_WORKER_STATUS_INTERNAL_ERROR;
                }
	            FreeErrorData(error_data);
	            pgstat_report_activity(STATE_IDLE, NULL);

	        }
	        PG_END_TRY();

	        MemoryContextSwitchTo(work_context);
	        MemoryContextReset(extra_memory_context_worker);

	        pgstat_report_stat(true);
	        pgstat_report_activity(STATE_IDLE, NULL);

	        // обновить статус задачи
	        // обновить статус воркера
	        LWLockAcquire(WorkersArr->lock , LW_EXCLUSIVE);
	            WorkersArr->Workers[index].TaskStatus = task_status;
	            WorkersArr->Workers[index].WorkerStatus = worker_status;
	        LWLockRelease(WorkersArr->lock );

	        // Сигнал основном потоку
	        SetLatch(&GetPGProcByNumber(WorkersArr->EngineProcNumber)->procLatch);
	    }
	    else
	    {
	        elog(LOG, "DWEn.%02d>> DEBUG Worker empty cycle", index);
	    }
	}
}