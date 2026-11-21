[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791)](https://www.postgresql.org/)
[![Version](https://img.shields.io/badge/version-0.2-orange)](https://github.com/kiramontus/dwen_light)
[![License](https://img.shields.io/badge/license-PostgreSQL-green)](LICENSE)
&emsp;&emsp;**English** | [Русский](README.ru.md)


# DataWarehouse Engine Light version
PostgreSQL extension to run SQL, PL/pgSQL tasks in background workers.

> [!WARNING]
> **Early Preview / Unstable:**  This extension is under active development. Features may change, and you may encounter bugs or unstable behavior.

## Description

Prototype of DataWarehouse framework.

Development and testing environment is Ubuntu 26.04.1 LTS and PostgreSQL 18.6.

## 1 Build & install


### 1.1 Build with PGXS (recommended)
#### 1.1.1 Prerequisites

* PostgreSQL 18 server
* pg_config and header files

```
sudo apt update
sudo apt install postgresql-server-dev-18
```
Verify that the header file exists:
```
ls -l $(pg_config --includedir-server)/postgres.h
```

#### 1.1.2 Build & install
In the directory with the downloaded repository execute 
```
sudo make PGXS=1
sudo make install USE_PGXS=1
```
#### 1.1.3 Proceed to the "Configure" section.
### 1.2 Alternative: build without PGXS
#### Prerequisites

* PostgreSQL server
* PostgreSQL repository with build dependencies installed

#### 1.2.1 Configure Makefile
In the Makefile inside the dwen_light repository directory, set `top_builddir` to the PostgreSQL source directory:
```
top_builddir = /home/user/pg_source
```

#### 1.2.2 Build 
In the dwen_light repository directory, run:
```
sudo make
```
#### 1.2.3 Install
Copy files to PostgreSQL
```
sudo cp dwen_light--0.2.sql /usr/share/postgresql/18/extension/
sudo cp dwen_light.control /usr/share/postgresql/18/extension/
sudo cp dwen_light.so /usr/lib/postgresql/18/lib/
```
#### 1.2.4 Proceed to the "Configure" section

### 1.3 Install pre-built extension
#### Prerequisites
* PostgreSQL server
#### 1.3.1 Download
* dwen_light.so
* dwen_light--0.2.sql
* dwwen_light.control
#### 1.3.2 Copy files to PostgreSQL
```
sudo cp dwen_light--0.2.sql /usr/share/postgresql/18/extension/
sudo cp dwen_light.control /usr/share/postgresql/18/extension/
sudo cp dwen_light.so /usr/lib/postgresql/18/lib/
```

## 2 Configure
### 2.1 postgresql.conf
```
cd /etc/postgresql/18/main/
```
Add to postgresql.conf
```
shared_preload_libraries = 'dwen_light'
```
At the end of the file in section CUSTOMIZED OPTIONS
```
dwen.max_execution_workers = 2
dwen.database = 'postgres'
dwen.role = 'dwen'
```


### 2.2 Restart PostgreSQL
```
sudo systemctl restart postgresql
sudo systemctl status postgresql
```
Errors in the log after restarting are expected at this stage.

### 2.3 PSQL - CREATE EXTENSION
```
sudo -i -u postgres
psql
```
```
CREATE EXTENSION dwen_light;
```

### 2.4 Restart PostgreSQL
If everything is configured correctly, the system log will look similar to this:

    LOG:  DWEn.ew>> DataWarehouse Engine initialization.
    LOG:  DWEn.ew>> Engine worker started with PID 163640
    LOG:  DWEn.ew>> Connection to database [dwen.database]: 'postgres' with role [dwen.role]: 'dwen'.
    LOG:  DWEn.ew>> Execution workers to run [max_dwen_workers]: 2
    LOG:  DWEn.ew>> DataWarehouse Engine initialized.
    LOG:  DWEn.00>> Execution worker index[00] started with PID 163642
    LOG:  DWEn.01>> Execution worker index[01] started with PID 163643


## 3 Test 
Create a new test task:
```
    insert into dwen.task(
        status,
        proc,
        execute_string,
        run_timestamp)
    VALUES (
        'READY',
        'X',
        'call dwen.dev_test_proc()',
        null);
```
The task will be launched within the next 15 seconds.

View the log:
```
select event_at, type, message, task_id, errcode
from dwen.log order by event_at desc;
```

## Author

Kirill Ostrovnoy

## Version History

* 0.2
    * Stable run strings
* 0.1
    * Skipped due to unstable background worker performance

## License

Distributed under the [PostgreSQL License](LICENSE)