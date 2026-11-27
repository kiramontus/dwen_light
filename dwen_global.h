/*  */
/*  Created by creator on 23.10.2026. */
/*  */

#ifndef PG_DWF_DWEN_GLOBAL_H
#define PG_DWF_DWEN_GLOBAL_H

#include "postgres.h"
#include "storage/procnumber.h"
#include "storage/lwlock.h"
#include "postmaster/bgworker.h"


#define DWEN_WORKER_STATUS_UNDEFINED 0
#define DWEN_WORKER_STATUS_STARTUP 1
#define DWEN_WORKER_STATUS_ACTIVE 2
#define DWEN_WORKER_STATUS_ERROR_SPI 3
#define DWEN_WORKER_STATUS_ERROR_DBO 4
#define DWEN_WORKER_STATUS_STOPPED 5
// ADD TO MONITOR!!!!

#define DWEN_WORKER_TASK_STATUS_UNDEFINED 0
#define DWEN_WORKER_TASK_STATUS_WAIT 1
#define DWEN_WORKER_TASK_STATUS_NEW 2
#define DWEN_WORKER_TASK_STATUS_EXECUTION 3
#define DWEN_WORKER_TASK_STATUS_DONE 4 // FIN
#define DWEN_WORKER_TASK_STATUS_ERROR 5 // FIN Произошла ошибка внтури кода из dwen.task.execute_string
#define DWEN_WORKER_TASK_STATUS_CANCELED 6 // FIN Выполнение кода отменено, транзакция не зафиксирована
#define DWEN_WORKER_TASK_STATUS_WARNING 7 // Ошибка модификации
#define DWEN_WORKER_TASK_STATUS_TIMEOUT 8 // FIN Превышен таймаут по задаче из dwen.task.timeout_minutes
// ADD TO MONITOR!!!!


/* value cached, fetched from shared memory */
/* static uint32 dwen_engine_wait_event_main = 0; */
static int	worker_spi_naptime = 20;


typedef struct DWEnWorker
{
	BackgroundWorker pg_BackgroundWorker;
	BackgroundWorkerHandle *pg_BackgroundWorkerHandle;
	ProcNumber	WorkerProcNumber;
	pid_t		pid;
	uint8_t		WorkerStatus;
	uint8_t		TaskStatus;
	uint32_t	TaskId;
	int			TimeoutTimer;
	int			TimeoutTreshhold;
    uint8_t		start_counter; // Количество перезапусков процесса при превышении 2х неудачных перезапусков воркер принудительно останавливается и более не используется
}			DWFWorker;

#define DWEN_MAX_WORKERS 4

typedef struct DWEnWorkers
{
	LWLock	   *lock;
	struct DWEnWorker Workers[DWEN_MAX_WORKERS];
	ProcNumber	EngineProcNumber;
	bool		EngineActive;
}			DWEnWorkers;

extern DWEnWorkers * WorkersArr;
/* extern LWLock*      LogContextLWLock; */

typedef struct worktable
{
	const char *schema;
	const char *name;
} worktable;

typedef struct
{
	LWLock	   *lock;
	int			PrevMinutes;
	bool		TickTimers;
}			DWEnEngineStateType;

extern DWEnEngineStateType * DWEnEngineState;


#define DWEN_LOG_TRACE 0
#define DWEN_LOG_INFO 1
#define DWEN_LOG_WARNING 2
#define DWEN_LOG_ERROR 3
#define DWEN_LOG_SYSERROR 4

void		dwen_log_event(uint8_t event_type, const char *log_message, uint32_t task_id, char error_code[const 5]);
void		dwen_suspend_engine();

#endif							/* //PG_DWF_DWEN_GLOBAL_H */
