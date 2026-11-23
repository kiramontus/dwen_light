#include "dwen_worker.h"
#include <stddef.h>
#include "dwen_global.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postgres.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"


PGDLLEXPORT pg_noreturn void dwen_worker_executor(Datum main_arg);

static  MemoryContext extra_memory_context_worker = NULL;
#define DWEN_EXECUTION_ERROR_SPI (1<<0)
#define DWEN_EXECUTION_ERROR_DWEN_DBO (1<<1)
#define DWEN_EXECUTION_ERROR_DWEN_DBO (1<<1)
static volatile sig_atomic_t dwen_worker_sigterm = false;

void dwen_worker_executor(Datum main_arg)
{
    int            index = DatumGetInt32(main_arg);
    char           log_msg[120];
    bits32         flags = 0;
    StringInfoData query;
    int32_t        task_id;
    uint8_t        task_status;
    uint8_t        worker_status;
    char  WaitEventExtension_txt[20];
    pid_t WorkerPID;
    bool dwen_worker_sigterm = false;

    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    pqsignal(SIGINT, StatementCancelHandler);
//+++ написать обработчики
    BackgroundWorkerUnblockSignals();

    /* Connect to database */
    BackgroundWorkerInitializeConnection(dwen_database, dwen_role,  flags);

    initStringInfo(&query);

    // Установка статуса "готов к работе" в регистре потоков
    LWLockAcquire(WorkersArr->lock , LW_EXCLUSIVE);
    WorkersArr->Workers[index].WorkerStatus = DWEN_WORKER_STATUS_ACTIVE;
    WorkersArr->Workers[index].TaskStatus = task_status = DWEN_WORKER_TASK_STATUS_WAIT;
    WorkersArr->Workers[index].TaskId = 0;
    WorkersArr->Workers[index].WorkerProcNumber = MyProcNumber;
    WorkerPID = WorkersArr->Workers[index].pid;
    LWLockRelease(WorkersArr->lock );

    sprintf(log_msg,"DWEn.%02d>> Executor worker index[%02d] started with PID %d", index, index, WorkerPID);
    elog(LOG, "%s", log_msg);
    dwen_log_event(DWEN_LOG_INFO, log_msg,0,"00000");

	for (;;)
	{
		bool is_error_dbo;
		bool is_error_spi;
		bool is_error_timeout;
		int   ret;
	    bool task_done_error;

	    /* Allocate or get the custom wait event */
	    sprintf(WaitEventExtension_txt,"DWEn worker [%02d]",index);
	    if (dwen_worker_wait_event_main == 0)
	        dwen_worker_wait_event_main = WaitEventExtensionNew(WaitEventExtension_txt);

	    WaitLatch(MyLatch,
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
	        WorkersArr->Workers[index].WorkerStatus == DWEN_WORKER_STATUS_ACTIVE)
	    {
	        task_id = WorkersArr->Workers[index].TaskId;
	        WorkersArr->Workers[index].TaskStatus = task_status = DWEN_WORKER_TASK_STATUS_EXECUTION;
	    }
	    else
	    {
	        task_id = 0;
	    }
	    LWLockRelease(WorkersArr->lock );

	    if (task_id > 0)
	    {
	        elog(LOG, "DWEn.%02d>> DEBUG Task to run: %d", index, task_id);

	        resetStringInfo(&query);
	        appendStringInfo(&query,
            "CALL dwen.task_execute_string(p_task_id := %d, error := NULL)",task_id);

	        MemoryContext work_context;
	        if (extra_memory_context_worker == NULL)
	            extra_memory_context_worker = AllocSetContextCreate(TopMemoryContext,
                                                               "DWEn executor worker context",
                                                               ALLOCSET_DEFAULT_SIZES);

	        MemoryContextReset(extra_memory_context_worker);
	        work_context = MemoryContextSwitchTo(extra_memory_context_worker);

	        SetCurrentStatementStartTimestamp();
	        StartTransactionCommand();
	        SPI_connect();
	        PushActiveSnapshot(GetTransactionSnapshot());
	        debug_query_string = query.data;
	        pgstat_report_activity(STATE_RUNNING, query.data);

	    	is_error_dbo = false;
	    	is_error_spi = false;
	        worker_status = DWEN_WORKER_STATUS_UNDEFINED;

	        PG_TRY();
	        {
	            ret = SPI_execute(query.data, false, 0);
	            debug_query_string = NULL;

	            if (ret >0 ) // Нет ошибки SPI
	            {
	                if (SPI_processed  == 1)
	                {
	                    // Вернулась одна запись - dwen.task_execute_string вернул значение
	                    // Oid type_oid = ;
                        if (SPI_gettypeid(SPI_tuptable->tupdesc, 1) == BOOLOID) // тип возвращенного результата -  boolean
                        {
	                        bool  is_null;
	                        task_done_error = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null));
	                        if (!is_null)
	                        {
	                            if (!task_done_error)
	                                task_status = DWEN_WORKER_TASK_STATUS_DONE;
	                            else
	                                task_status = DWEN_WORKER_TASK_STATUS_ERROR; // Ошибка возникла при выполнении задачи
	                            // Ошибка перехвачена в dwen.task_execute_string в блок EXCEPTION
	                            // Ошибка SQL / plpgsql записывется в лог процедурой
	                        }
	                        else // Если запрос вернул строку, но там null - это поломанная функция dwen.task_execute_string
	                            is_error_dbo = true;
                        }
	                    else
	                        is_error_dbo = true;
	                }
	                else // Процедура dwen.task_execute_string не вернула значение или вернула больше одной строки
                        is_error_dbo = true;
	            }
	            else //  Ошибка SPI
	            {
	            	is_error_spi = true;
	            }

	        	SPI_finish();
	        	PopActiveSnapshot();

	        	if (!is_error_spi && !is_error_dbo)
	        	{
	        		CommitTransactionCommand();
	        	}
	        	else
	        	{
	        		AbortCurrentTransaction();
	        	    task_status = DWEN_WORKER_TASK_STATUS_CANCELED;

	        	    if (is_error_spi)
	        	    {
	        	        worker_status = DWEN_WORKER_STATUS_ERROR_SPI;
	        	        ereport(LOG,
                                errmsg("DWEn.%02d>> ERROR SPI: SPI_execute return %d. Engine worker has been suspended." ,index, ret)
                                );


	        	        // todo Добавить внутреннй тип ошибки в лог DWEN и передавать ее туда
	        	        //+ запись в таблицу log
	        	    }
	        	    else if (is_error_dbo)
	        	    {
	        	        worker_status = DWEN_WORKER_STATUS_ERROR_DBO;
	        	        ereport(LOG,
                                errmsg("DWEn.%02d>> ERROR Procedure 'dwen.task_execute_string' was modified incorrectly.",index)
                                );
	        	        //+ запись в таблицу log
	        	    }
	        	}
	        }
	        PG_CATCH();
	        {
	        	debug_query_string = NULL;
	            ErrorData* error_data;
	            MemoryContextSwitchTo(extra_memory_context_worker);
	            error_data = CopyErrorData();
	            FlushErrorState();
	            LWLockReleaseAll();
	            AbortCurrentTransaction();
	            MemoryContextSwitchTo(extra_memory_context_worker);

	            if (error_data->sqlerrcode == ERRCODE_QUERY_CANCELED) // Внешнее прерывание выполнения задачи
	            {
	                task_status = DWEN_WORKER_TASK_STATUS_TIMEOUT;
	                ereport(LOG,
                            errcode(error_data->sqlerrcode),
                            errmsg("DWEn.%02d>> Task %d execution timed out.", index, task_id)
                            );

	                //++ в лог!!!
	            }
	            // Получен сигнал SIGTERM
	            else if (error_data->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
	            {
	                dwen_worker_sigterm = true;
	                task_status = DWEN_WORKER_TASK_STATUS_CANCELED;
	                worker_status = DWEN_WORKER_STATUS_STOPPED;
	            }
                else // другие ошибки вызова процедуры
                {
                    task_status = DWEN_WORKER_TASK_STATUS_CANCELED;
                    worker_status = DWEN_WORKER_STATUS_ERROR_DBO;

                    debug_query_string = query.data;
                    ereport(LOG,
                            errcode(error_data->sqlerrcode),
                            errmsg("DWEn.%02d>> Task %d execution error: %s %s", index, unpack_sql_state(error_data->sqlerrcode), error_data->message));
                    debug_query_string = NULL;


                    //+ запись в таблицу log
                }
	            FreeErrorData(error_data);
	            pgstat_report_activity(STATE_IDLE, NULL);
	        }
	        PG_END_TRY();

	        MemoryContextSwitchTo(work_context);
	        MemoryContextReset(extra_memory_context_worker);

	        pgstat_report_stat(true); // todo проверить а нужна ли она тут?
	        pgstat_report_activity(STATE_IDLE, NULL);

	        if (!dwen_worker_sigterm)
	        {
	            LWLockAcquire(WorkersArr->lock , LW_EXCLUSIVE);
	            WorkersArr->Workers[index].TaskStatus = task_status; // обновить статус задачи
	            if (worker_status != DWEN_WORKER_STATUS_UNDEFINED)
	                WorkersArr->Workers[index].WorkerStatus = worker_status; // обновить статус воркера
	            LWLockRelease(WorkersArr->lock );

	            // Сигнал основному потоку
	            SetLatch(&GetPGProcByNumber(WorkersArr->EngineProcNumber)->procLatch);
	        }
	        else
	        {
	            LWLockAcquire(WorkersArr->lock , LW_EXCLUSIVE);
	            WorkersArr->Workers[index].TaskStatus = DWEN_WORKER_TASK_STATUS_CANCELED; // обновить статус задачи
	            WorkersArr->Workers[index].WorkerStatus = DWEN_WORKER_STATUS_STOPPED; // обновить статус воркера
	            LWLockRelease(WorkersArr->lock );

	            ereport(LOG, (errmsg("DWEn.%02d>> Execution worker got SIGTERM. Task %d status will be returned to READY" , index,task_id)));
	            proc_exit(0);
	        }
	    }
	    // else
	    // {
	    //    Поток разбудули не из-за новой задачи или контроллер прислал выполнять задачу 0
	    // }
	}
}