//
// Created by creator on 24.10.2026.
//

#include "dwen_engine.h"
#include "dwen_global.h"
//#include <string.h>
#include <time.h>
#include "postgres.h"


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
#include "commands/dbcommands.h"
#include "utils/memutils.h"

PGDLLEXPORT pg_noreturn void dwen_worker_engine(Datum main_arg);

#define DWEN_TASK_STATUS_READY 0
#define DWEN_TASK_STATUS_RUN 1
#define DWEN_TASK_STATUS_DONE 2
#define DWEN_TASK_STATUS_ERROR 3
#define DWEN_TASK_STATUS_WARNING 4
#define DWEN_TASK_STATUS_TIMEOUT 5


static MemoryContext  extra_memory_context_engine = NULL;
static StringInfoData query;
static volatile sig_atomic_t dwen_worker_engine_sigterm = false;

static void set_task_status(uint32_t task_id, uint8_t task_status);
static void launch_executor_worker(int index);

static void dwen_worker_engine_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    dwen_worker_engine_sigterm = true;
    SetLatch(MyLatch);

    errno = save_errno;
}

void dwen_worker_engine(Datum main_arg)
{
    char*          p;
    bits32         flags = 0;

    uint8_t  WorkerCurrentStatus;
    uint8_t  TaskCurrentStatus;
    int32_t worker_task_id;
    int32_t worker_timeout_treshhold;

    ProcNumber PN;
    bool       check_ready_task;
    StringInfoData log_msg;
    StringInfoData query_eng;

    initStringInfo(&query_eng);
    initStringInfo(&query);
    initStringInfo(&log_msg);

    // для запуска динамического процесса
    BackgroundWorker        worker;
    BackgroundWorkerHandle *worker_handle;
    BgwHandleStatus         status;
    pid_t                   pid;
    Size                    ndim;
    BackgroundWorkerHandle* handle;
    //


    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, dwen_worker_engine_sigterm_handler);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();
    elog(LOG, "DWEn.ew>> DataWarehouse Engine initialization.");
    elog(LOG, "DWEn.ew>> Engine worker started with PID %d", MyProcPid);
    elog(LOG, "DWEn.ew>> Connection to database [dwen.database]: '%s' with role [dwen.role]: '%s'.",
         dwen_database,
         dwen_role);

    // В случае ошибок в dwen_database/dwen_role Поток падает в ошибку и в логе это видно:
    BackgroundWorkerInitializeConnection(dwen_database, dwen_role, flags);

    resetStringInfo(&log_msg);
        appendStringInfo(&log_msg,
                "DWEn.ew>> Execution workers to run [max_dwen_workers]: %d",
                max_dwen_workers
                 );

   // elog(LOG, log_msg.data);
    dwen_log_event(DWEN_LOG_INFO, log_msg.data, 0, "00000");

    WorkersArr->EngineProcNumber = MyProcNumber;

    // Запуск исполнительных процессов
    for (int ew_ind = 0; ew_ind < dwen_active_execution_workers_count; ew_ind++)
    {
        launch_executor_worker(ew_ind);
    }

    elog(LOG, "DWEn.ew>> DataWarehouse Engine initialized.");
    dwen_log_event(DWEN_LOG_INFO, "DWEn.ew>> DataWarehouse Engine initialized.", 0, "00000");


    time_t now = time(NULL);
    struct tm *local = localtime(&now);
    DWEnEngineState->PrevMinutes = local->tm_min;
    DWEnEngineState->TickTimers = false;

    /*
     * Main loop: do this until SIGTERM is received and processed by
     * ProcessInterrupts.
     */
    for
    (;;)
    {
        int ret;
        int wl_ret;
        bool is_active;
        pid_t pid;

        /* First time, allocate or get the custom wait event */
        if (dwen_engine_wait_event_main == 0)
           dwen_engine_wait_event_main = WaitEventExtensionNew("DWFEngineMain");

        wl_ret = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                           15000L, dwen_engine_wait_event_main);
        ResetLatch(MyLatch);
        // Вызвана команда заврешения процесса управляющего воркера
        if (!dwen_worker_engine_sigterm)
        {
            // 1. Проверяем, было ли пробуждение по SetLatch
            if (wl_ret & WL_LATCH_SET)
            {
                // Сбрасываем защелку для следующего использования

                elog(LOG, "DWEn.ew>> DEBUG Worker callback execution");
                // Ваша логика обработки события SetLatch
            }

            // 2. Проверяем, произошло ли пробуждение по таймауту
            if (wl_ret & WL_TIMEOUT)
            {
                elog(LOG, "DWEn.ew>> DEBUG Timeout execution");
                dwen_log_event(DWEN_LOG_INFO, "DWEn ENG >>> TIMER CYCLE", 0, "00000");
            }

            if (wl_ret & WL_POSTMASTER_DEATH)
            {
                // Процесс postmaster завершился — нужно выходить
                proc_exit(1);
            }

            CHECK_FOR_INTERRUPTS();

            /*
             * In case of a SIGHUP, just reload the configuration.
             */
            if (ConfigReloadPending)
            {
                ConfigReloadPending = false;
                ProcessConfigFile(PGC_SIGHUP);
                // Изменить базу и пользователя под которым подключаестся основной поток можно только при перезапуске сервера
                // Для подтверждения возможности смены БД "на лету" нужен эксперимент а пока делаем это невозможным

                // Изменить количество потоков "на лету" без перезапуска сервера вполне возможно
                // Может понадобиться для экспериментов под нагрузкой без необходимости перезапуска
                // Реализовать отдельной фичей
            }

            time_t now = time(NULL);
            local = localtime(&now);
            //  elog(LOG, "DWEn.ew>> local->tm_min) = %d", local->tm_min);
            if (DWEnEngineState->PrevMinutes != local->tm_min)
            {
                //elog(LOG, "DWEn.ew>> MIN + 1 ");
                DWEnEngineState->PrevMinutes = local->tm_min;
                DWEnEngineState->TickTimers = true;
            }




            // Для будущей версии: Вызов процедуры обновления условий запуска по времени будет выполняться отсюда

            check_ready_task = true;

            // Прочесываем пул исполнительных процессов
            for (int w_ind = 0; w_ind < dwen_active_execution_workers_count; w_ind++)
            {
                // Получаем актуальную информацию по потоку
                LWLockAcquire(WorkersArr->lock, LW_SHARED);
                WorkerCurrentStatus = WorkersArr->Workers[w_ind].WorkerStatus;
                TaskCurrentStatus = WorkersArr->Workers[w_ind].TaskStatus;
                worker_task_id = WorkersArr->Workers[w_ind].TaskId;
                worker_timeout_treshhold = WorkersArr->Workers[w_ind].TimeoutTreshhold;
                worker_handle = WorkersArr->Workers[w_ind].pg_BackgroundWorkerHandle;
                is_active = WorkersArr->EngineActive;
                LWLockRelease(WorkersArr->lock);

                /* Неблокирующая проверка текущего состояния */


                // STOP HERE
                // Проверка потока на ошибки
                if (WorkerCurrentStatus == DWEN_WORKER_STATUS_ACTIVE)
                {
                    status = GetBackgroundWorkerPid(worker_handle, &pid);
                    switch (status)
                    {
                        case BGWH_STARTED:
                            /* Воркер жив и работает, его PID записан в переменную pid */
                            elog(LOG, "DWEn.ew>> W[%02d] status = BGWH_STARTED", w_ind);
                            break;

                        case BGWH_NOT_YET_STARTED:
                            /* Воркер зарегистрирован, но postmaster его еще не запустил */
                            elog(LOG, "DWEn.ew>> W[%02d] status = BGWH_NOT_YET_STARTED", w_ind);
                            break;

                        case BGWH_STOPPED:
                            /* Воркер завершился (Убили или упал с ошибкой, которую не удалось поймать) */

                            // Воркер выполнял задачу и возникла ошибка
                            if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_EXECUTION)
                                set_task_status(worker_task_id,DWEN_TASK_STATUS_READY); /* Сбрасываем статус задачи */

                            LWLockAcquire(WorkersArr->lock, LW_SHARED);
                            WorkersArr->Workers[w_ind].WorkerStatus = DWEN_WORKER_STATUS_STOPPED;
                            WorkersArr->Workers[w_ind].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
                            LWLockRelease(WorkersArr->lock);

                            elog(LOG, "DWEn.ew>> W[%02d] status = BGWH_STOPPED", w_ind);

                            break;

                        case BGWH_POSTMASTER_DIED:
                            /* Управляющий процесс postmaster умер */
                            elog(LOG, "DWEn.ew>> W[%02d] status = BGWH_POSTMASTER_DIED", w_ind);
                            break;
                    }
                    // if (status == BGWH_STARTED) // Процесс живой
                    // { /// А эту проверку возможно стоит делать для любого статуса воркера, ну кроме STOP - подумать
                    //
                    // }
                    //
                    // else if (status == BGWH_STOPPED) // был вызов kill -TERM
                    //     /// Вот это и нужно проуерить: воркер системой убивается - по идее его нужно запутсить - нужно пробовать
                }
                else if (WorkerCurrentStatus == DWEN_WORKER_STATUS_STOPPED
                    && TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_CANCELED)
                {
                    set_task_status(worker_task_id,DWEN_TASK_STATUS_READY); /* Сбрасываем статус задачи */

                    LWLockAcquire(WorkersArr->lock, LW_SHARED);
                    WorkersArr->Workers[w_ind].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
                    LWLockRelease(WorkersArr->lock);
                }
                else if (WorkerCurrentStatus == DWEN_WORKER_STATUS_ERROR_SPI)
                {
                    // Задачу возвращаем в статус готовности к выполнению, т.к. она точно не выполнилась

                    set_task_status(worker_task_id,DWEN_TASK_STATUS_READY);

                    LWLockAcquire(WorkersArr->lock, LW_SHARED);
                    WorkersArr->Workers[w_ind].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
                    LWLockRelease(WorkersArr->lock);

                    dwen_log_event(DWEN_LOG_ERROR, "Task execution aborted due internal error", worker_task_id, "00000");

                    // Получается мы исполнительный поток подвесили и нового на него ничего не подаем
                    // и движок приостановлен. При возобновлении движка поток останется висеть в ошибочном статусе
                    // todo Сделать перезапуск ошибочных потоков ++ ) А пока будут висеть мертвым грузом
                }
                else if (WorkerCurrentStatus == DWEN_WORKER_STATUS_ERROR_DBO) ////
                {
                    dwen_suspend_engine(); ////

                    set_task_status(worker_task_id,DWEN_TASK_STATUS_READY);

                    LWLockAcquire(WorkersArr->lock, LW_SHARED);
                    WorkersArr->Workers[w_ind].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
                    LWLockRelease(WorkersArr->lock);

                    ereport(LOG,
                            errmsg("DWEn.ew>> ERROR Engine worker has been suspended."),
                            errhint(
                                "After the cause is resolved, call procedure 'select dwen.set_engine_status_active(true)' to resume the process."
                            ));
                }
                else // с потоком все хорошо - обрабатываем
                {
                    // Если поток обработал задачу
                    if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_DONE
                        || TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_ERROR)
                    {
                        if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_DONE)
                        {
                            set_task_status(worker_task_id,DWEN_TASK_STATUS_DONE);
                            dwen_log_event(DWEN_LOG_TRACE, "Task done.", worker_task_id, "00000");
                        }
                        else if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_ERROR)
                        {
                            set_task_status(worker_task_id,DWEN_TASK_STATUS_ERROR);
                            dwen_log_event(DWEN_LOG_TRACE, "Error while executing task", worker_task_id, "00000");
                        }
                        TaskCurrentStatus = DWEN_WORKER_TASK_STATUS_WAIT;

                        LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
                        WorkersArr->Workers[w_ind].TaskId = 0;
                        WorkersArr->Workers[w_ind].TaskStatus = TaskCurrentStatus;
                        LWLockRelease(WorkersArr->lock);

                        check_ready_task = true;
                    }
                    else if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_TIMEOUT)
                    {
                        set_task_status(worker_task_id,DWEN_TASK_STATUS_TIMEOUT);
                        TaskCurrentStatus = DWEN_WORKER_TASK_STATUS_WAIT;
                    }

                    //  elog(LOG, "DWEn.ew>> W[%02d]TaskCurrentStatus = %d", w_ind, TaskCurrentStatus);
                    if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_EXECUTION // поток выполняется
                        && DWEnEngineState->TickTimers == true // щелкнул таймер
                        && worker_timeout_treshhold > 0) // и для задачи установлен таймаут
                    {
                        bool task_timeout = false;
                        pid_t worker_pid;
                        // int dbg_tt;// DBG
                        // int dbg_ttr;// DBG
                        LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
                        WorkersArr->Workers[w_ind].TimeoutTimer = WorkersArr->Workers[w_ind].TimeoutTimer +1;
                        if (WorkersArr->Workers[w_ind].TimeoutTimer > WorkersArr->Workers[w_ind].TimeoutTreshhold)
                        {
                            task_timeout = true;
                            worker_pid = WorkersArr->Workers[w_ind].pid;
                        }
                        // dbg_tt = WorkersArr->Workers[w_ind].TimeoutTimer;// DBG
                        // dbg_ttr = WorkersArr->Workers[w_ind].TimeoutTreshhold; // DBG
                        LWLockRelease(WorkersArr->lock);
                        // elog(LOG, "DWEn.ew>> W[%02d] TimeoutTimer = %d, TimeoutTreshhold = %d", w_ind, dbg_tt, dbg_ttr); // DBG
                        if (task_timeout)
                        {
                            if (kill(worker_pid, SIGINT) == 0)
                            {
                                elog(LOG, "DWEn.ew>> W[%02d] execution worker task timeout, SIGINT sended to PID %d",w_ind, worker_pid);
                            }
                        }
                    }
                }
                // Перезапуск процессов
                if (is_active && WorkerCurrentStatus == DWEN_WORKER_STATUS_ERROR_SPI) // Если движок активен
                {
                    BackgroundWorkerHandle *handle;
                    pid_t pid;
                    LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
                    handle = WorkersArr->Workers[w_ind].pg_BackgroundWorkerHandle;
                    pid = WorkersArr->Workers[w_ind].pid;
                    LWLockRelease(WorkersArr->lock);

                    elog(LOG, "DWEn.ew>> Restarting execution worker [%02d] with current PID %d.",w_ind, pid);

                    TerminateBackgroundWorker(handle);
                    WaitForBackgroundWorkerShutdown(handle);

                    launch_executor_worker(w_ind);
                }

                // elog(LOG, "DWEn.ew>> is_active = %d",is_active);
                if (is_active) // Если движок активен
                {
                    if (TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_WAIT && check_ready_task)
                    {
                        // Поток готов к обработке новой задачи
                        //int32 task_id = DWEnSetNextTaskToWorker(w_ind);

                        if (DWEnSetNextTaskToWorker(w_ind)) // Нашлась задача, готовая к заупску
                        {
                            // Будим исполнительный процесс
                            LWLockAcquire(WorkersArr->lock, LW_SHARED);
                            PN = WorkersArr->Workers[w_ind].WorkerProcNumber;
                            LWLockRelease(WorkersArr->lock);
                            SetLatch(&GetPGProcByNumber(PN)->procLatch);
                        }
                        else // Нет задач, готовых к заупску
                        {
                            // Сброс флага наличия в очереди задач, готовых к запуску
                            check_ready_task = false;

                            elog(LOG, "DWEn.ew>> DEBUG no tasks in READY status");
                        }
                    }
                }
                else // EngineActive = false
                {
                    elog(LOG, "DWEn.ew>> DataWarehouse engine suspended");
                }
            }
            DWEnEngineState->TickTimers = false; // Сброс флага минутного таймера
        }
        else // dwen_worker_engine_sigterm = true
        {
            ereport(LOG, (errmsg("DWEn.ew>> Engine worker got SIGTERM. Shutting down executor workers.")));
            // Завершаем все исполнительные процессы
            for (int ind = 0; ind < dwen_active_execution_workers_count; ind++)
            {
                LWLockAcquire(WorkersArr->lock, LW_SHARED);
                WorkerCurrentStatus = WorkersArr->Workers[ind].WorkerStatus;
                TaskCurrentStatus = WorkersArr->Workers[ind].TaskStatus;

                /* Сбрасываем статус воркера и задачи */
                WorkersArr->Workers[ind].WorkerStatus = DWEN_WORKER_STATUS_STOPPED;
                WorkersArr->Workers[ind].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;

                worker_task_id = WorkersArr->Workers[ind].TaskId;
                pid = WorkersArr->Workers[ind].pid;

                LWLockRelease(WorkersArr->lock);
                if (WorkerCurrentStatus != DWEN_WORKER_STATUS_STOPPED)
                {
                    if (kill(pid, SIGTERM) == 0)
                    {
                        elog(LOG, "DWEn.ew>> Sent SIGTERM to execution worker [%02d] / PID %d ",ind, pid);
                    }

                    if (WorkerCurrentStatus == DWEN_WORKER_STATUS_ACTIVE
                        && TaskCurrentStatus == DWEN_WORKER_TASK_STATUS_EXECUTION)
                    {
                        /* Сбрасываем статус задачи */
                        set_task_status(worker_task_id,DWEN_TASK_STATUS_READY);
                    }
                }
            }
            //+++ Добавить установку флага для перезапуска через команду
            ereport(LOG, (errmsg("DWEn.ew>> All workers terminated, exiting cleanly."),
                        errhint("Restart the PostgreSQL server to run the DataWarehouse engine again.")));
            proc_exit(0);
        }
    }

    /* Not reachable */
}

const char* query_get_next_task_id =
    "merge into dwen.task  trg "
    "using (select id, timeout_minutes from dwen.task where status = 'READY' order by id limit 1) src "
    "    on trg.id = src.id "
    "    when matched then update set status = 'RUN', run_timestamp = CLOCK_TIMESTAMP() "
    "returning trg.id, src.timeout_minutes";

bool DWEnSetNextTaskToWorker(int worker_index)
{
    //  elog(LOG, "DWEn ENG >>> next_task -8");
    int32 next_task_id = 0;
    int32 timeout_minutes;
    bool new_task_found = false;

    MemoryContext work_context;
    // elog(LOG, "DWEn ENG >>> 000-1");
    if (extra_memory_context_engine == NULL)
        extra_memory_context_engine = AllocSetContextCreate(TopMemoryContext,
                                                            "DWEn engine worker context",
                                                            ALLOCSET_DEFAULT_SIZES);

    // elog(LOG, "DWEn ENG >>> 0000");
    MemoryContextReset(extra_memory_context_engine);
    work_context = MemoryContextSwitchTo(extra_memory_context_engine);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    // if (SPI_connect() <> SPI_OK_CONNECT)
    // {
    // }
    PushActiveSnapshot(GetTransactionSnapshot());
    // elog(LOG, "DWEn ENG >>> 0001 %s",query_get_next_task_id);
    //query_get_next_task_id
    //elog(LOG, "DWEn.ew>> Worker %d CT 001",worker_index);
    PG_TRY();
        {
            int res;

            // debug_query_string = query_eng.data;
            pgstat_report_activity(STATE_RUNNING, query_get_next_task_id);

            if (SPI_exec(query_get_next_task_id, 0) < 0)
                elog(ERROR, "SPI_exec returned unexpected negative status"); // todo поправить логику ошибки

            if (SPI_processed > 0)
            {
                bool isnull;
                next_task_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                                           SPI_tuptable->tupdesc, 1, &isnull));

                timeout_minutes = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                                           SPI_tuptable->tupdesc, 2, &isnull));
                //++ Проврека на null и на больше или равно  нулю!!!!

                LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
                WorkersArr->Workers[worker_index].TaskId = next_task_id;
                WorkersArr->Workers[worker_index].TaskStatus = DWEN_WORKER_TASK_STATUS_NEW;
                WorkersArr->Workers[worker_index].TimeoutTimer = 0;
                WorkersArr->Workers[worker_index].TimeoutTreshhold = timeout_minutes;
                LWLockRelease(WorkersArr->lock);
                new_task_found = true;
                elog(LOG, "DWEn.ew>> NewTask  %d to Worker %d",next_task_id,worker_index); // todo поправить логику ошибки
                //  elog(LOG, "DWEn ENG >>> 0003");

            }
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            pgstat_report_activity(STATE_IDLE, NULL);
        }
    PG_CATCH();
        {
            ErrorData* error_data;
            // elog(LOG, "DWEn ENG >>> 0005555");
            MemoryContextSwitchTo(extra_memory_context_engine);

            error_data = CopyErrorData();

            FlushErrorState();
            LWLockReleaseAll();
            AbortCurrentTransaction();
            MemoryContextSwitchTo(extra_memory_context_engine);

            dwen_suspend_engine();

            debug_query_string = query_get_next_task_id;
            ereport(LOG,
                    errcode(error_data->sqlerrcode),
                    errmsg("DWEn.ew>> ERROR %s %s", unpack_sql_state(error_data->sqlerrcode), error_data->message));
            debug_query_string = NULL;
            ereport(LOG,
                    errmsg("DWEn.ew>> ERROR Engine worker has been suspended."),
                    errhint(
                        "After the cause is resolved, call procedure 'select dwen.set_engine_status_active(true)' to resume the process."
                    ));

            FreeErrorData(error_data);
            pgstat_report_activity(STATE_IDLE, NULL);
        }
    PG_END_TRY();
    MemoryContextSwitchTo(work_context);
    MemoryContextReset(extra_memory_context_engine);
    debug_query_string = NULL;
    // elog(LOG, "DWEn ENG >>> 99");
    return new_task_found;
}



const char task_status_ready_txt[] = "READY";
const char task_status_run_txt[] = "RUN";
const char task_status_done_txt[] = "DONE";
const char task_status_error_txt[] = "ERROR";
const char task_status_warning_txt[] = "WARNING";
const char task_status_timeout_txt[] = "TIMEOUT";


static void set_task_status(uint32_t task_id, uint8_t task_status)
{
    char* new_status_txt;

    switch (task_status)
    {
    case DWEN_TASK_STATUS_READY:
        new_status_txt = task_status_ready_txt;
        break;

    case DWEN_TASK_STATUS_RUN:
        new_status_txt = task_status_run_txt;
        break;

    case DWEN_TASK_STATUS_DONE:
        new_status_txt = task_status_done_txt;
        break;

    case DWEN_TASK_STATUS_ERROR:
        new_status_txt = task_status_error_txt;
        break;

    case DWEN_TASK_STATUS_WARNING:
        new_status_txt = task_status_warning_txt;
        break;

    case DWEN_TASK_STATUS_TIMEOUT:
        new_status_txt = task_status_timeout_txt;
    }

    resetStringInfo(&query);
    if (task_status == DWEN_TASK_STATUS_RUN)
        appendStringInfo(&query,
                 "update dwen.task set status = '%s', run_timestamp = clock_timestamp() where id = %d",
                 new_status_txt,
                 task_id
                 );
        else
    appendStringInfo(&query,
                     "update dwen.task set status = '%s' where id = %d",
                     new_status_txt,
                     task_id
                     );

    MemoryContext work_context;

    if (extra_memory_context_engine == NULL)
        extra_memory_context_engine = AllocSetContextCreate(TopMemoryContext,
                                                            "DWEn engine worker context",
                                                            ALLOCSET_DEFAULT_SIZES);

    // elog(LOG, "DWEn ENG >>> 0000");
    MemoryContextReset(extra_memory_context_engine);
    work_context = MemoryContextSwitchTo(extra_memory_context_engine);
   // elog(LOG, "-----> 1");
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    // if (SPI_connect() <> SPI_OK_CONNECT)
    // {
    // }
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
        {
            int res;

            //debug_query_string = query.data;
            pgstat_report_activity(STATE_RUNNING, query.data);
            if (SPI_exec(query.data, 0) < 0)
                elog(ERROR, "SPI_exec returned unexpected negative status"); // todo поправить логику ошибки


            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            pgstat_report_activity(STATE_IDLE, NULL);
        }
    PG_CATCH();
        {
            ErrorData* error_data;
            // elog(LOG, "DWEn ENG >>> 0005555");
            MemoryContextSwitchTo(extra_memory_context_engine);

            error_data = CopyErrorData();

            FlushErrorState();
            LWLockReleaseAll();
            AbortCurrentTransaction();
            MemoryContextSwitchTo(extra_memory_context_engine);
       // elog(LOG, "-----> 4");
            dwen_suspend_engine();

            debug_query_string = query.data;
            ereport(LOG,
                    errcode(error_data->sqlerrcode),
                    errmsg("DWEn.ew>> ERROR %s %s", unpack_sql_state(error_data->sqlerrcode), error_data->message));
            debug_query_string = NULL;
            ereport(LOG,
                    errmsg("DWEn.ew>> Engine worker has been suspended."),
                    errhint(
                        "After the cause is resolved, execute 'select dwen.set_engine_status_active(true)' to resume the engine worker."
                    ));

            FreeErrorData(error_data);
            pgstat_report_activity(STATE_IDLE, NULL);
        }
    PG_END_TRY();
    MemoryContextSwitchTo(work_context);
    MemoryContextReset(extra_memory_context_engine);
    debug_query_string = NULL;
}

void launch_executor_worker(int index)
{
    BgwHandleStatus         status;
    // elog(LOG, "DWEn.ew>> !!000");
    LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);

    WorkersArr->Workers[index].pg_BackgroundWorker.bgw_flags = BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    WorkersArr->Workers[index].pg_BackgroundWorker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    WorkersArr->Workers[index].pg_BackgroundWorker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(WorkersArr->Workers[index].pg_BackgroundWorker.bgw_library_name, "dwen_light");
    sprintf(WorkersArr->Workers[index].pg_BackgroundWorker.bgw_function_name, "dwen_worker_executor");
    snprintf(WorkersArr->Workers[index].pg_BackgroundWorker.bgw_name, BGW_MAXLEN,
             "pg_dwen executor worker [%02d]",
             index);
    snprintf(WorkersArr->Workers[index].pg_BackgroundWorker.bgw_type, BGW_MAXLEN, "pg_dwen");
    WorkersArr->Workers[index].pg_BackgroundWorker.bgw_main_arg = Int32GetDatum(index);
    WorkersArr->Workers[index].pg_BackgroundWorker.bgw_notify_pid = MyProcPid;

    if (RegisterDynamicBackgroundWorker(&WorkersArr->Workers[index].pg_BackgroundWorker,
                                        &WorkersArr->Workers[index].pg_BackgroundWorkerHandle))
    {
        // Удалось запустить запускающий поток
        status = WaitForBackgroundWorkerStartup(WorkersArr->Workers[index].pg_BackgroundWorkerHandle,
                                                &WorkersArr->Workers[index].pid);

        if (status == BGWH_STARTED)
        {
            WorkersArr->Workers[index].WorkerStatus = DWEN_WORKER_STATUS_STARTUP;
            WorkersArr->Workers[index].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
            //elog(LOG, "WF worker[%02d] started with PID %d", index, WorkersArr->Workers[index].pid);
            // ereport(NOTICE,
            //         (errmsg("DWEn >> execution worker[%02d] started with PID %d", index, WorkersArr->Workers[index].pid)));
        }
        else if (status == BGWH_STOPPED)
        {
            WorkersArr->Workers[index].WorkerStatus = DWEN_WORKER_STATUS_UNDEFINED;
            WorkersArr->Workers[index].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
            elog(ERROR, "DWEn.ew>> Execution worker[%02d] could not start: BGWH_STOPPED while startup", index);
        }
        else
        {
            WorkersArr->Workers[index].WorkerStatus = DWEN_WORKER_STATUS_UNDEFINED;
            WorkersArr->Workers[index].TaskStatus = DWEN_WORKER_TASK_STATUS_UNDEFINED;
            elog(ERROR, "DWEn.ew>> Execution worker[%02d] could not start: unexpected error", index);
        }
    }
    else // Не удалось запустить запускающий поток
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                    errmsg("DWEn.ew>> worker[%02d] register failure",1),
                    errhint(
                        "Increase the parameter value [max_worker_processes] or decrease [dwen.max_execution_workers] in postgres.config"
                    )));
    }
    LWLockRelease(WorkersArr->lock);
}