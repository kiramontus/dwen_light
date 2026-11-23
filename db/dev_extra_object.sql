drop table dwen.dev_tab;
CREATE TABLE if not exists dwen.dev_tab
(
    id              integer generated always as identity not null,
    field_int       integer                              null,
    field_text      text                                 null,
    field_timestamp timestamp                            null
);
GRANT ALL PRIVILEGES ON TABLE dwen.dev_tab TO dwen;

-----------------------------------------------------------------------------------------------------------------------
-- DWEN Процедура загрузки
------------------------------------------------------------------------------------------------------------------------

DROP PROCEDURE dwen.dev_test_proc;
CREATE OR REPLACE PROCEDURE dwen.dev_test_proc(
--     OUT p_error_code CHAR(5) -- код ошибки
) AS
$$
--DECLARE

BEGIN

    insert into dwen.dev_tab (field_int, field_text, field_timestamp)
    VALUES (999, 'qweqwe qwe qwe', clock_timestamp());
    PERFORM PG_SLEEP(120);
END;

$$ LANGUAGE plpgsql;

insert into dwen.task
(status,
 proc,
 execute_string,
 timeout_minutes)
VALUES ('READY',
        'X',
        'call dwen.dev_test_proc()',
        10);

GRANT ALL PRIVILEGES ON ROUTINE dwen.dev_test_proc TO dwen;

DO
$$
--DECLARE

    BEGIN

        call dwen.dev_test_proc();
    END;

$$ LANGUAGE plpgsql;

select *
from dwen.dev_tab;