/*  */
/*  Created by Kirill Ostrovnoy on 08.11.2026. */
/*  */
#include "dwen_global.h"

#include "pgstat.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

/*  Когда объявление в dwen_global.c  - то каждый процесс, который вызывает запись в лог создает */
/*  и потом переиспользует свой экземпляр контектса */
static MemoryContext extra_memory_context_log = NULL;

void
dwen_log_event(uint8_t event_type, const char *log_message, uint32_t task_id, char error_code[const 5])
{
	MemoryContext work_context;
	StringInfoData query;
	int			ret;
	char		event_type_trace[] = "TRACE";
	char		event_type_info[] = "INFO";
	char		event_type_warning[] = "WARNING";
	char		event_type_error[] = "ERROR";
	char		event_type_syserr[] = "SYSERROR";
	char	   *event_type_text;

	switch (event_type)
	{
		case DWEN_LOG_TRACE:
			event_type_text = event_type_trace;
			break;

		case DWEN_LOG_INFO:
			event_type_text = event_type_info;
			break;

		case DWEN_LOG_WARNING:
			event_type_text = event_type_warning;
			break;

		case DWEN_LOG_ERROR:
			event_type_text = event_type_error;
			break;

		case DWEN_LOG_SYSERROR:
			event_type_text = event_type_syserr;
	}

	initStringInfo(&query);
	appendStringInfo(&query, "INSERT INTO dwen.log (event_at, type, message, task_id, errcode) "
					 "VALUES (CLOCK_TIMESTAMP(), '%s', '%s', %d, '%s')", event_type_text, log_message, task_id,
					 error_code);

	if (extra_memory_context_log == NULL)
		extra_memory_context_log = AllocSetContextCreate(TopMemoryContext,
														 "DWEn engine worker log context",
														 ALLOCSET_DEFAULT_SIZES);
	MemoryContextReset(extra_memory_context_log);
	work_context = MemoryContextSwitchTo(extra_memory_context_log);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	PG_TRY();
	{
		/* debug_query_string = query.data; */
		pgstat_report_activity(STATE_RUNNING, query.data);

		ret = SPI_exec(query.data, 0);
		if (ret < 0)
		{
			dwen_suspend_engine();
			ereport(LOG,
					errmsg("DWEn.log>> ERROR SPI: SPI_execute return %d. Engine worker has been suspended.", ret)
				);
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(extra_memory_context_log);

		ErrorData  *error_data;

		error_data = CopyErrorData();

		FlushErrorState();
		LWLockReleaseAll();
		AbortCurrentTransaction();

		MemoryContextSwitchTo(extra_memory_context_log);

		dwen_suspend_engine();

		debug_query_string = query.data;
		ereport(LOG,
				errcode(error_data->sqlerrcode),
				errmsg("DWEn ERROR>> %s %s", unpack_sql_state(error_data->sqlerrcode), error_data->message));
		debug_query_string = NULL;
		ereport(LOG,
				errmsg("DWEn ERROR>> Engine worker has been paused."),
				errhint(
						"After the cause is resolved, call procedure 'select dwen.set_engine_status_active(true)' to resume the process."
						));

		FreeErrorData(error_data);
		pgstat_report_activity(STATE_IDLE, NULL);
	}
	PG_END_TRY();
	MemoryContextSwitchTo(work_context);
	MemoryContextReset(extra_memory_context_log);
}

void
dwen_suspend_engine()
{
	LWLockAcquire(WorkersArr->lock, LW_EXCLUSIVE);
	WorkersArr->EngineActive = false;
	LWLockRelease(WorkersArr->lock);

}
