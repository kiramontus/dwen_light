-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dwen_light" to load this file. \quit

DO
$$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles
            WHERE rolname = 'dwen'
        ) THEN
            CREATE ROLE dwen;
        END IF;
    END
$$;

-- 1. Права на саму схему (если еще не выданы)
GRANT ALL PRIVILEGES ON SCHEMA dwen TO dwen;

-- 2. Права по умолчанию на будущие таблицы (включая представления и материализованные представления)
ALTER DEFAULT PRIVILEGES IN SCHEMA dwen
    GRANT ALL PRIVILEGES ON TABLES TO dwen;

-- 3. Права по умолчанию на будущие последовательности (sequences)
ALTER DEFAULT PRIVILEGES IN SCHEMA dwen
    GRANT ALL PRIVILEGES ON SEQUENCES TO dwen;

-- 4. Права по умолчанию на будущие функции и процедуры (routines)
ALTER DEFAULT PRIVILEGES IN SCHEMA dwen
    GRANT ALL PRIVILEGES ON ROUTINES TO dwen;

-- 5. Права по умолчанию на будущие пользовательские типы данных
ALTER DEFAULT PRIVILEGES IN SCHEMA dwen
    GRANT ALL PRIVILEGES ON TYPES TO dwen;

DROP TABLE IF EXISTS dwen.task;
CREATE TABLE dwen.task
(
    id             integer generated always as identity not null,
    status         varchar(10)                          not null,
    proc           varchar(300)                         null,
    execute_string varchar(500)                         null,
    run_timestamp  timestamp                            null,
    timeout_minutes int NOT NULL DEFAULT 60
);

DROP PROCEDURE IF EXISTS dwen.task_execute_string(
    IN p_task_id INTEGER, -- Номер задачи
    OUT error BOOLEAN
);

CREATE OR REPLACE PROCEDURE dwen.task_execute_string(
    IN p_task_id INTEGER, -- Номер задачи
    OUT error BOOLEAN
) AS
$$
DECLARE
    v_sql text;
BEGIN
    select execute_string into v_sql from dwen.task where id = p_task_id;

    EXECUTE v_sql;

    CALL dwen.log_event(p_event_type := 'INFO',
                        p_event_text := '[task_execute_string]  TASK DONE',
                        p_task_id := p_task_id,
                        p_errcode := '00000');
    error := false;
EXCEPTION
    WHEN OTHERS THEN CALL dwen.log_event(p_event_type := 'ERROR',
                                         p_event_text := '[task_execute_string] ' || sqlerrm,
                                         p_task_id := p_task_id,
                                         p_errcode := sqlstate);

        error := true;
END;

$$ LANGUAGE plpgsql;


CREATE TABLE dwen.log
(
    event_at TIMESTAMP   NOT NULL,
    type     VARCHAR(10) NOT NULL,
    message  TEXT        NOT NULL,
    task_id  INTEGER     NOT NULL,
    errcode  CHAR(5)     NOT NULL
);

CREATE OR REPLACE PROCEDURE dwen.log_event(
    IN p_event_type varchar(10),
    IN p_event_text varchar(3000),
    IN p_task_id integer,
    IN p_errcode char(5) default '00000')
AS
$$
BEGIN
    INSERT INTO dwen.log (event_at,
                          type,
                          message,
                          task_id,
                          errcode)
    VALUES (CLOCK_TIMESTAMP(),
            p_event_type,
            p_event_text,
            p_task_id,
            p_errcode);
END;
$$ LANGUAGE plpgsql;

-- TEST TABLE AND PROCEDURE
DROP TABLE IF EXISTS wen.dev_tab;
CREATE TABLE dwen.dev_tab
(
    id              integer generated always as identity not null,
    field_int       integer                              null,
    field_text      text                                 null,
    field_timestamp timestamp                            null
);
GRANT ALL PRIVILEGES ON TABLE dwen.dev_tab TO dwen;

CREATE OR REPLACE PROCEDURE dwen.dev_test_proc(
) AS
$$
BEGIN

    insert into dwen.dev_tab (field_int, field_text, field_timestamp)
    VALUES (999, 'test text', clock_timestamp());
--PERFORM PG_SLEEP(20);
END;
$$ LANGUAGE plpgsql;
-- END TEST TABLE AND PROCEDURE

GRANT ALL PRIVILEGES ON ROUTINE dwen.dev_test_proc TO dwen;


CREATE or REPLACE FUNCTION dwen.get_engine_status_active()
    RETURNS pg_catalog.bool
    STRICT
AS
'MODULE_PATHNAME',
'get_engine_status_active'
    LANGUAGE c;

CREATE or REPLACE FUNCTION dwen.set_engine_status_active(new_state BOOLEAN)
    RETURNS pg_catalog.bool
    STRICT
AS
'MODULE_PATHNAME',
'set_engine_status_active'
    LANGUAGE c;

CREATE or REPLACE FUNCTION dwen.dwen_resume_engine()
    RETURNS cstring
AS
'MODULE_PATHNAME',
'dwen_resume_engine'
    LANGUAGE c;

CREATE FUNCTION dwen.dwen_status_monitor()
    RETURNS TABLE (worker text, status text, task_status text, task_id int, minutes_to_timeout int)
AS 'MODULE_PATHNAME', 'dwen_status_monitor'
    LANGUAGE C VOLATILE;