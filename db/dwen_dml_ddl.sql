CREATE ROLE  if not exists dwen LOGIN;
ALTER ROLE dwen PASSWORD 'dwenPASS26';
-- CREATE DATABASE dwen_PROT;
-- ALTER DATABASE dwen_PROT OWNER TO dwen;
CREATE USER creator WITH SUPERUSER PASSWORD 'cre##pwd';
-----
-- CREATE SCHEMA int_extsys;

CREATE SCHEMA dwen;
ALTER SCHEMA dwen OWNER TO dwen;

drop SCHEMA dwen CASCADE;

drop table if exists dwen.task;
CREATE TABLE dwen.task
(
    id             integer generated always as identity not null,
    status         varchar(10)                          not null,
    proc           varchar(300)                         null,
    execute_string varchar(500)                         null,
    run_timestamp  timestamp                            null,
    timeout_minutes int NOT NULL DEFAULT 60
);

ALTER TABLE dwen.task ADD CONSTRAINT dwen_task_timeout_minutes_nn
check ( timeout_minutes >=0 );

alter table dwen.task add COLUMN  timeout_minute int NOT NULL DEFAULT 60;
alter table dwen.task    rename column timeout_minute to timeout_minutes;

insert into dwen.task
(status,
 proc,
 execute_string,
timeout_minutes)
VALUES ('READY',
        'X',
        'call dwen.dev_test_proc()',
        1);
insert into dwen.task
(status,
 proc,
 execute_string,
 run_timestamp)
VALUES ('READY',
        'X',
        'call dwen.dev_test_proc()',
        null);
insert into dwen.task
(status,
 proc,
 execute_string,
 run_timestamp)
VALUES ('READY',
        'X',
        'call dwen.dev_test_proc()',
        null);



------------------------------------------------------------------------------------------------------------------------
-- DWF Процедура загрузки stage-таблицы
------------------------------------------------------------------------------------------------------------------------

DROP PROCEDURE dwen.task_execute_string;
CREATE OR REPLACE PROCEDURE dwen.task_execute_string(
    IN p_task_id INTEGER, -- Номер задачи
    OUT error BOOLEAN
) AS
$$
DECLARE
    v_sql text;
BEGIN
    select execute_string into v_sql from dwen.task where id = p_task_id;

    EXECUTE v_sql; -- Запуск процедуры загрузки данных

    --update dwen.task set status = 'DONE' where id = p_task_id;

    CALL dwen.log_event(p_event_type := 'INFO',
                        p_event_text := '[task_execute_string]  TASK DONE',
                        p_task_id := p_task_id,
                        p_errcode := '00000');
    error := FALSE;
   -- error := NULL;
EXCEPTION
    WHEN OTHERS THEN CALL dwen.log_event(p_event_type := 'ERROR',
                                         p_event_text := '[task_execute_string] ' || sqlerrm,
                                         p_task_id := p_task_id,
                                         p_errcode := sqlstate);

      --  update dwen.task set status = 'ERROR' where id = p_task_id;

        error := true;
END;

$$ LANGUAGE plpgsql;
--SANDBOX


select execute_string
into v_sql
from dwen.task
where id =

DO
$$
    DECLARE
        v_task_id    integer;
        v_error_code CHAR(5);
        v_error_text text;
        v_is_error   boolean;
    BEGIN

        CALL dwen.task_execute_string(p_task_id := 1, error := v_is_error);
        raise notice 'TASK error = %', v_is_error;
    END;

$$ LANGUAGE plpgsql;

select *
from dwen.task
order by id desc;

-- Этот блок созадается и работает до момента перевода всех процедур DIRECT на 3 параметра
-- по количеству параметров в хранимой процедуре определяем она относится к старому типу или новому
--         BEGIN
--             SELECT
--                 pronargs
--             INTO STRICT
--                 v_arg_num
--             FROM pg_proc
--             WHERE
--                 proname = SUBSTR(v_procedure_name, POSITION('.' IN v_procedure_name) + 1);
--         EXCEPTION
--             WHEN OTHERS THEN
--                 v_arg_num := 1; -- считаем что старый тип процедуры
--         END;

--         IF v_arg_num = 1 THEN -- Старый тип процедуры с 2я параметрами (p_batch_id => $1, p_error_code := $2)


-- RAISE NOTICE c_procedure_name ||'> pre get diagnostics';
--     GET DIAGNOSTICS v_row_count := ROW_COUNT;
--     IF v_row_count = 0 THEN -- В stage-таблицу не вставлено ни одной строки
--p_error_code := 'XI101';
--         CALL iemk_dwf.prc_log_add(p_event_type := 'WARNING',
--                                   p_event_text := '[' || c_procedure_name || ']  #',
--                                   p_task_id := p_task_id,
--                                   p_batch_id := p_batch_id,
--                                   p_errcode := p_error_code);

--         p_error_code := '00000';

--
-- v_sql := 'CALL ' || v_procedure_name || ' (p_batch_id => $1, p_error_code := $2)';
-- EXECUTE v_sql INTO v_call_error_code USING v_batch_id, v_call_error_code; -- Запуск процедуры загрузки данных

select *
from dwen.task
order by id desc;





------------------------------------------------------------------------------------------------------------------------
-- DWEN.EXECUTE_STRING
------------------------------------------------------------------------------------------------------------------------


DROP PROCEDURE dwen.execute_string;
CREATE OR REPLACE PROCEDURE dwen.execute_string(
    IN p_task_id INTEGER -- Номер задачи
) AS
$$
DECLARE
    v_execute_string text;
--     c_procedure_name CONSTANT TEXT := 'stage.prc_dwf_iemk_dct_medical_card_mkab_s';
--     v_row_count               INTEGER;
BEGIN
    select execute_string
    into
        v_execute_string
    from dwen.task
    where id = p_task_id;
--     v_sql := 'CALL ' || v_procedure_name || ' (p_batch_id => $1, p_error_code := $2)';
    EXECUTE v_execute_string; -- Запуск процедуры загрузки данных


EXCEPTION
    --     WHEN OTHERS THEN CALL iemk_dwf.prc_log_add(p_event_type := 'ERROR',
--                                                p_event_text := '[' || c_procedure_name || '] ' || sqlerrm,
--                                                p_task_id := p_task_id,
--                                                p_batch_id := p_batch_id,
--                                                p_errcode := sqlstate);
--         p_error_code = sqlstate;
END;

$$ LANGUAGE plpgsql;
