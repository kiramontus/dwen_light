CREATE ROLE DWF LOGIN;
ALTER ROLE DWF PASSWORD 'DWFPASS26';
CREATE DATABASE DWF_PROT;
ALTER DATABASE DWF_PROT OWNER TO DWF;

-----
CREATE SCHEMA int_extsys;

CREATE SCHEMA dwf;
ALTER SCHEMA dwf OWNER TO DWF;

CREATE TABLE dwf.task
(
    id             integer generated always as identity not null,
    status         varchar(10)                          not null,
    proc           varchar(300)                         null,
    execute_string varchar(500)                         null,
    run_timestamp  time                                 not null
);

CREATE TABLE dwf.log
(
    id         integer generated always as identity not null,
    event_ts   timestamp                            not null,
    event_type varchar(10)                          not null,
    event      text
);


insert into dwf.task (status, proc, execute_string, run_timestamp)
values ('NEW',
        'asdadad', null,
        current_timestamp);