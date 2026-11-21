-- Логирование
DROP TABLE dwen.log;
CREATE TABLE dwen.log
(
    event_at timestamp   NOT NULL,
    type     varchar(10) NOT NULL,
    message  text        NOT NULL,
    task_id  integer     NOT NULL,
    errcode  char(5)     NOT NULL
);
--
-- COMMENT ON TABLE dwen.load_log IS 'Лог загрузки';
-- COMMENT ON COLUMN dwen.load_log.ts_eventта/время события';
-- COMMENT ON COLUMN dwen.load_log.event_type_code IS 'Код типа события';
-- COMMENT ON COLUMN dwen.load_log.batch_id IS 'ID пачки';
-- COMMENT ON COLUMN dwen.load_log.task_id IS 'ID задачи';
-- COMMENT ON COLUMN dwen.load_log.errcode IS 'Код ошибки';

-- alter table dwen.log
--     owner to dwen;

--SANDBOX

DROP PROCEDURE dwen.log_event;
CREATE OR REPLACE PROCEDURE dwen.log_event(
    IN p_event_type varchar(10),
    IN p_event_text varchar(3000),
    IN p_task_id integer,
    IN p_errcode char(5) default '00000')
AS
$$
DECLARE
    v_error_text text;
    v_event_text text;
    v_row_count  integer;
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

--SANDBOX
DO
$$
    DECLARE
        v_task_id    integer;
        v_error_code CHAR(5);
        v_error_text text;
    BEGIN

        CALL dwen.log_event(p_event_type := 'TEST', p_event_text := 'dsfSDfsdfDDasd      Fs', p_task_id := 0);
    END;

$$ LANGUAGE plpgsql;


select *
from dwen.log
order by event_at desc;