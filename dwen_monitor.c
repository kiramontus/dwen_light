//
// Created by creator on 24.11.2026.
//

#include "dwen_monitor.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "dwen_global.h"


extern int dwen_active_execution_workers_count;
typedef struct {
    int current;
    int max;
} TextSrfState;

PG_FUNCTION_INFO_V1(dwen_status_monitor);

Datum
dwen_status_monitor(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int max_rows = 4;

    /* 1. Проверяем, поддерживает ли вызывающий контекст режим Materialize */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("функция должна вызываться в контексте, принимающем набор строк")));

    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("режим материализации (SFRM_Materialize) не поддерживается")));

    /* 2. Получаем описание колонок возвращаемой таблицы */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("функция должна возвращать составной тип (таблицу)")));

    /* 3. Переключаемся в контекст памяти уровня всего запроса (per_query_ctx) */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /* Инициализируем Tuplestore (будет использовать память до work_mem, затем временный файл) */
    tupstore = tuplestore_begin_heap(true, false, work_mem);

    /* Настраиваем ReturnSetInfo для возврата результата */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    /* Возвращаемся в обычный контекст вызова функции, чтобы временные строки очищались */
    MemoryContextSwitchTo(oldcontext);

    /* 4. В цикле формируем и записываем все строки в Tuplestore */
    //(worker text, status text, task_status text, task_id int, minutes_to_timeout int)
    for (int w_ind = 0; w_ind < dwen_active_execution_workers_count; w_ind++)
    {
        Datum values[5];
        bool nulls[5] = {false, false, false, false, false};
        char *str1;
        char *str2;
        char *str3;
        uint8_t  worker_current_status;
        uint8_t  task_current_status;
        int32_t worker_task_id;
        int32_t timeout_timer;
        int32_t timeout_trashhold;
        int32_t timeout;

        LWLockAcquire(WorkersArr->lock, LW_SHARED);
        worker_current_status = WorkersArr->Workers[w_ind].WorkerStatus;
        task_current_status = WorkersArr->Workers[w_ind].TaskStatus;
        worker_task_id = WorkersArr->Workers[w_ind].TaskId;
        timeout_timer = WorkersArr->Workers[w_ind].TimeoutTimer;
        timeout_trashhold = WorkersArr->Workers[w_ind].TimeoutTreshhold;
        LWLockRelease(WorkersArr->lock);

        /* Создаем временные C-строки */
        str1 = psprintf("DWEn executor [%02d]", w_ind);

        switch (worker_current_status)
        {
            case DWEN_WORKER_STATUS_UNDEFINED:
                str2 = pstrdup("DWEN_WORKER_STATUS_UNDEFINED");
                break;
            case DWEN_WORKER_STATUS_STARTUP:
                str2 = pstrdup("DWEN_WORKER_STATUS_STARTUP");
                break;
            case DWEN_WORKER_STATUS_ACTIVE:
                str2 = pstrdup("DWEN_WORKER_STATUS_ACTIVE");
                break;
            case DWEN_WORKER_STATUS_ERROR_SPI:
                str2 = pstrdup("DWEN_WORKER_STATUS_ERROR_SPI");
                break;
            case DWEN_WORKER_STATUS_ERROR_DBO:
                str2 = pstrdup("DWEN_WORKER_STATUS_ERROR_DBO");
                break;
            case DWEN_WORKER_STATUS_STOPPED:
                str2 = pstrdup("DWEN_WORKER_STATUS_STOPPED");
                break;
            default:
                str2 = psprintf("%d", worker_current_status);
        }

        switch (task_current_status)
        {
            case DWEN_WORKER_TASK_STATUS_UNDEFINED:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_UNDEFINED");
                break;
            case DWEN_WORKER_TASK_STATUS_WAIT:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_WAIT");
                break;
            case DWEN_WORKER_TASK_STATUS_NEW:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_NEW");
                break;
            case DWEN_WORKER_TASK_STATUS_EXECUTION:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_EXECUTION");
                break;
            case DWEN_WORKER_TASK_STATUS_DONE:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_DONE");
                break;
            case DWEN_WORKER_TASK_STATUS_ERROR:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_ERROR");
                break;
            case DWEN_WORKER_TASK_STATUS_CANCELED:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_CANCELED");
                break;
            case DWEN_WORKER_TASK_STATUS_WARNING:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_WARNING");
                break;
            case DWEN_WORKER_TASK_STATUS_TIMEOUT:
                str3 = pstrdup("DWEN_WORKER_TASK_STATUS_TIMEOUT");
                break;
            default:
                str3 = psprintf("%d", task_current_status);
        }

        /* Преобразуем в text* */
        values[0] = PointerGetDatum(cstring_to_text(str1));
        values[1] = PointerGetDatum(cstring_to_text(str2));
        values[2] = PointerGetDatum(cstring_to_text(str3));
        values[3] = Int32GetDatum(worker_task_id);
        if (timeout_trashhold>0)
            timeout = timeout_trashhold - timeout_timer;
        else
            timeout = -1;
        values[4] = Int32GetDatum(timeout);
        /* Кладем строку в Tuplestore (функция сама скопирует данные в свой контекст) */
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);

        /* Освобождаем временные объекты текущей итерации, чтобы не копить память в цикле */
        pfree(DatumGetPointer(values[0]));
        pfree(DatumGetPointer(values[1]));
        pfree(DatumGetPointer(values[2]));
        pfree(str1);
        pfree(str2);
        pfree(str3);
    }

    /* 5. Сигнализируем Postgres, что данные готовы (в режиме Materialize возвращается (Datum) 0) */
    return (Datum) 0;
}