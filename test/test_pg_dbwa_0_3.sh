#!/bin/bash

# Сценарий регрессионного тестирования функций расширения pg_dbwa.
# Предполагается запуск сценария на пустом экземпляре без других активностей.


# Настройки теста

export test_version="0.3.1"
export test_prev_version="0.3"
export test_dbname_new="pg_dbwa_test_new"
export test_dbname_upd="pg_dbwa_test_upd"

export v_psql_cmd="psql"
export v_pgbench_cmd="pgbench"


# Операции создания и обновленимя расширения

test_db_create() {
# Создание тестовой базы данных
# test_db_create <dbname>

    if [ -z "$1" ]; then
        echo
        echo "test_db_create: argument check failed"
        echo
        exit 1;
    fi

    v_dbname="$1"
    echo
    echo "test_db_create: $v_dbname"
    echo
    $v_psql_cmd -e -d postgres -c "CREATE DATABASE $v_dbname OWNER = postgres;"

    return $?

}

test_db_drop() {
# Удаление тестовой базы данных
# test_db_drop <dbname>

    if [ -z "$1" ]; then
        echo
        echo "test_db_drop: argument check failed"
        echo
        exit 1;
    fi

    v_dbname="$1"
    echo
    echo "test_db_drop: $v_dbname"
    echo    
    $v_psql_cmd -e -d postgres -c "DROP DATABASE IF EXISTS $v_dbname;"

    return $?

}

test_ext_create() {
# Создание расширения
# test_ext_create <dbname> <extension>

    if [ -z "$2" ]; then
        echo
        echo "test_ext_create: argument check failed"
        echo
        exit 1;
    fi

    v_dbname="$1"
    v_extension="$2"
    echo
    echo "test_ext_create: $v_dbname $v_extension"
    echo    
    $v_psql_cmd -e -d $v_dbname -c "CREATE EXTENSION $v_extension;"

    return $?

}

test_ext_create_version() {
# Создание расширения с указанием версии
# test_ext_create_version <dbname> <extension> <version>

    if [ -z "$3" ]; then
        echo
        echo "test_ext_create_version: argument check failed"
        echo
        exit 1;
    fi

    v_dbname="$1"
    v_extension="$2"
    v_version="$3"
    echo
    echo "test_ext_create_version: $v_dbname $v_extension $v_version"
    echo    
    $v_psql_cmd -e -d $v_dbname -c "CREATE EXTENSION $v_extension
WITH VERSION '$v_version';"

    return $?

}

test_ext_update() {
# Обновление расширения до default_version
# test_ext_update <dbname>

    if [ -z "$2" ]; then
        echo
        echo "test_ext_update: argument check failed"
        echo
        exit 1;
    fi

    v_dbname="$1"
    v_extension="$2"
    echo
    echo "test_ext_update: $v_dbname $v_extension"
    echo    
    $v_psql_cmd -e -d $v_dbname -c "ALTER EXTENSION $v_extension UPDATE;"

    return $?

}

test_ext_check() {
# Проверка версии расширения
# test_ext_check <dbname> <extension> <version>

    if [ -z "$3" ]; then
        echo
        echo "test_ext_check: argument check failed"
        echo
        exit 1;
    fi

    v_dbname=$1
    v_extension="$2"
    v_version="$3"
    echo
    echo "test_ext_check: $v_dbname $v_extension $v_version"
    echo
    
    v_sql_text="SELECT count(*)
FROM pg_catalog.pg_extension e
WHERE e.extname = '$v_extension' AND e.extversion = '$v_version';"
    v_sql_return=`$v_psql_cmd -A -t -d $v_dbname -c "$v_sql_text"`

    if [ "$v_sql_return" == "1" ]; then
        echo "EXIST"
    else
        echo "NOT FOUND"
        exit 1
    fi

    return 0

}

test_pg_dbwa_settings_check() {
# Настройка основных параметров расширения
# test_pg_dbwa_settings_check <dbname> <clustername>

    if [ -z "$2" ]; then
        echo
        echo "test_pg_dbwa_settings_check: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    v_clustername="$2"
    echo
    echo "test_pg_dbwa_settings_check: $v_dbname $v_clustername"
    echo 

    v_return_status="0"
       
    $v_psql_cmd -e -d $v_dbname -c "SELECT *
FROM dbwa.cluster_config
WHERE clustername = '$v_clustername';"
    if [ "$?" != "0" ]; then v_return_status="1"; fi

    $v_psql_cmd -e -d $v_dbname -c "SELECT co.*
FROM dbwa.cluster_operation co
LEFT JOIN dbwa.cluster_config cc ON co.clusterid = cc.clusterid
WHERE cc.clustername = '$v_clustername';"
    if [ "$?" != "0" ]; then v_return_status="1"; fi

    return $v_return_status

}

test_pg_dbwa_settings_cluster_add() {
# Добавление настроек для удаленной БД
# test_pg_dbwa_settings_cluster_add <dbname> <clustername>

    if [ -z "$2" ]; then
        echo
        echo "test_pg_dbwa_settings_cluster_add: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    v_clustername="$2"
    echo
    echo "test_pg_dbwa_settings_cluster_add: $v_dbname $v_clustername"
    echo
    
    # Временное решение.  Будет заменено на вызов функции.
    v_sql_text="DO \$\$
DECLARE

    v_clusterid dbwa.cluster_config.clusterid%type;
    v_sql TEXT;
    
BEGIN
    v_clusterid := nextval('dbwa.clusterid_seq');

    v_sql := 'INSERT INTO dbwa.cluster_config (clusterid, clustername,
    rhost, rport, rdbname, rusername, rpassword, enabled)
    VALUES(\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8)';
    EXECUTE v_sql USING v_clusterid, '$v_clustername', '127.0.0.1', '5432',
        'pg_dbwa_test_new', 'dbwa', 'dbwa', 'Y';

    v_sql := 'INSERT INTO dbwa.cluster_operation (opid, clusterid, operation,
    enabled)
    VALUES(\$1, \$2, \$3, \$4)';
    EXECUTE v_sql USING nextval('dbwa.opid_seq'), v_clusterid,
        'dbwa.get_statements','Y';

END\$\$;"

    $v_psql_cmd -e -d $v_dbname -c "$v_sql_text"
    
    return $?

}

test_pgbench_init() {
# Инициализация объектов для стандартного теста pgbench
# test_pgbench_init <dbname> <scale>

    if [ -z "$2" ]; then
        echo
        echo "test_pgbench_init: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    v_scale="$2"
    echo
    echo "test_pgbench_init: $v_dbname $v_scale"
    echo

    # Инициализация объектов для теста
    echo $v_pgbench_cmd --initialize --scale=$v_scale $v_dbname
    $v_pgbench_cmd --initialize --scale=$v_scale $v_dbname
    
    return $?

}

test_pg_dbwa_stat_generate() {
# Генерация статистики
# test_pg_dbwa_stat_generate <dbname>

    if [ -z "$1" ]; then
        echo
        echo "test_pg_dbwa_stat_generate: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    echo
    echo "test_pg_dbwa_stat_generate: $v_dbname"
    echo

    v_return_status="0"
    
    v_pgbench_arg="--client=10 --time=60 --report-latencies"
    
    # Сброс статистики для pg_stat_statements
    $v_psql_cmd -e -d $v_dbname -c "SELECT pg_stat_statements_reset();"
    if [ "$?" != "0" ]; then exit 1; fi

    # Выполнение ряда тестов со сбором статистики между тестами
    for i in 10 15 20 25 30
    do
        echo $v_pgbench_cmd --rate=$i $v_pgbench_arg $v_dbname
        $v_pgbench_cmd --rate=$i $v_pgbench_arg $v_dbname
        if [ "$?" != "0" ]; then exit 1; fi
        $v_psql_cmd -e -d $v_dbname -c "BEGIN;
SELECT * FROM dbwa.get_stat('local');
SELECT * FROM dbwa.get_stat('rlocal');
COMMIT;"
        if [ "$?" != "0" ]; then exit 1; fi
    done

    return $v_return_status

}

test_pg_dbwa_stat_check() {
# Проверка наличия ошибок выполнения операций сбора статистики
# test_pg_dbwa_stat_check <dbname>

    if [ -z "$1" ]; then
        echo
        echo "test_pg_dbwa_stat_check: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    echo
    echo "test_pg_dbwa_stat_check: $v_dbname"
    echo 

    v_return_status="0"

    $v_psql_cmd -e -d $v_dbname -c "SELECT
    count(*) AS total,
    count(*) FILTER (WHERE stat_status = 'C') AS successfully,
    count(*) FILTER (WHERE stat_status != 'C') AS failure
FROM dbwa.stat_history;"
    if [ "$?" != "0" ]; then exit 1; fi

    v_sql_return=`$v_psql_cmd -A -t -d $v_dbname -c "WITH stat_status AS (SELECT
    count(*) AS total,
    count(*) FILTER (WHERE stat_status = 'C') AS successfully,
    count(*) FILTER (WHERE stat_status != 'C') AS failure
FROM dbwa.stat_history)
SELECT
    CASE
        WHEN total = successfully THEN 'SUCCESSFULLY'
        ELSE 'WITH FAILURE'
    END AS stat_status
FROM stat_status
;"`
    if [ "$?" != "0" ]; then exit 1; fi
    if [ "$v_sql_return" != "SUCCESSFULLY" ]; then
        v_return_status="1"
    fi

    echo "$v_sql_return"

    return $v_return_status

}

test_pg_dbwa_show_top_queryes() {
# Получение топа запросов по собранной статистике
# test_pg_dbwa_show_top_queryes <dbname> <clustername>

    if [ -z "$2" ]; then
        echo
        echo "test_pg_dbwa_show_top_queryes: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    v_clustername="$2"
    echo
    echo "test_pg_dbwa_show_top_queryes: $v_dbname $v_clustername"
    echo
    
    $v_psql_cmd -e -d $v_dbname -c "WITH range_time AS (SELECT
    min(stat_time) AS min_time,
    max(stat_time) AS max_time
FROM dbwa.stat_history sh
LEFT JOIN dbwa.cluster_config cc ON cc.clusterid = sh.clusterid
WHERE cc.clustername = 'local')
SELECT * FROM dbwa.show_top_queryes(
    '$v_clustername',
    (SELECT min_time FROM range_time),
    (SELECT max_time FROM range_time),
    5
);"
    
    return $?

}

test_pg_dbwa_show_stat_query() {
# Получение статистики по запросу.  Для теста выбирается самый тяжелый запрос.
# test_pg_dbwa_show_stat_query <dbname> <clustername>

    if [ -z "$2" ]; then
        echo
        echo "test_pg_dbwa_show_stat_query: argument check failed"
        echo
        exit 1;
    fi
    
    v_dbname="$1"
    v_clustername="$2"
    echo
    echo "test_pg_dbwa_show_stat_query: $v_dbname $v_clustername"
    echo
    
    $v_psql_cmd -e -d $v_dbname -c "WITH range_time AS (SELECT
    min(stat_time) AS min_time,
    max(stat_time) AS max_time
FROM dbwa.stat_history sh
LEFT JOIN dbwa.cluster_config cc ON cc.clusterid = sh.clusterid
WHERE cc.clustername = 'local'),
query_info AS (SELECT userid, dbid, queryid FROM dbwa.show_top_queryes(
    '$v_clustername',
    (SELECT min_time FROM range_time),
    (SELECT max_time FROM range_time), 1)
)
SELECT * FROM dbwa.show_stat_query(
    '$v_clustername',
    (SELECT userid FROM query_info),
    (SELECT dbid FROM query_info),
    (SELECT queryid FROM query_info),
    (SELECT min_time FROM range_time),
    (SELECT max_time FROM range_time)
);"
    
    return $?

}


# Выполнение теста

main() {

echo "pg_dbwa test"
echo "`date`"

echo
echo "test_version=$test_version"
echo "test_prev_version=$test_prev_version"
echo "test_dbname_new=$test_dbname_new"
echo "test_dbname_upd=$test_dbname_upd"
echo "v_psql_cmd=$v_psql_cmd"
echo "v_pgbench_cmd=$v_pgbench_cmd"


# Тест для новой инсталляции

test_db_drop $test_dbname_new || exit 1
test_db_create $test_dbname_new || exit 1

test_ext_create $test_dbname_new dblink || exit 1
test_ext_create $test_dbname_new pg_stat_statements || exit 1
test_ext_create $test_dbname_new pg_eyes || exit 1
test_ext_create $test_dbname_new pg_prttn_tools || exit 1
test_ext_create $test_dbname_new pg_dbwa || exit 1
test_ext_check $test_dbname_new pg_dbwa $test_version || exit 1

test_pg_dbwa_settings_check $test_dbname_new "local" || exit 1
test_pg_dbwa_settings_cluster_add $test_dbname_new "rlocal" || exit 1
test_pg_dbwa_settings_check $test_dbname_new "rlocal" || exit 1

test_pgbench_init $test_dbname_new 100 || exit 1
test_pg_dbwa_stat_generate $test_dbname_new || exit 1
test_pg_dbwa_stat_generate $test_dbname_new || exit 1
test_pg_dbwa_stat_check $test_dbname_new || exit 1

test_pg_dbwa_show_top_queryes $test_dbname_new "local" || exit 1
test_pg_dbwa_show_stat_query $test_dbname_new "local" || exit 1
test_pg_dbwa_show_top_queryes $test_dbname_new "rlocal" || exit 1
test_pg_dbwa_show_stat_query $test_dbname_new "rlocal" || exit 1


# Тест обновления с предыдущей версии

test_db_drop $test_dbname_upd || exit 1
test_db_create $test_dbname_upd || exit 1

test_ext_create $test_dbname_upd dblink || exit 1
test_ext_create $test_dbname_upd pg_stat_statements || exit 1
test_ext_create $test_dbname_upd pg_eyes || exit 1
test_ext_create $test_dbname_upd pg_prttn_tools || exit 1
test_ext_create_version $test_dbname_upd pg_dbwa $test_prev_version || exit 1
test_ext_check $test_dbname_upd pg_dbwa $test_prev_version || exit 1

test_pg_dbwa_settings_check $test_dbname_upd "local" || exit 1
test_pg_dbwa_settings_cluster_add $test_dbname_upd "rlocal" || exit 1
test_pg_dbwa_settings_check $test_dbname_upd "rlocal" || exit 1

test_pgbench_init $test_dbname_upd 100 || exit 1
test_pg_dbwa_stat_generate $test_dbname_upd || exit 1
test_pg_dbwa_stat_check $test_dbname_upd || exit 1

test_ext_update $test_dbname_upd pg_dbwa || exit 1
test_ext_check $test_dbname_upd pg_dbwa $test_version || exit 1

test_pg_dbwa_stat_generate $test_dbname_upd || exit 1
test_pg_dbwa_stat_check $test_dbname_upd || exit 1

test_pg_dbwa_show_top_queryes $test_dbname_upd "local" || exit 1
test_pg_dbwa_show_stat_query $test_dbname_upd "local" || exit 1
test_pg_dbwa_show_top_queryes $test_dbname_upd "rlocal" || exit 1
test_pg_dbwa_show_stat_query $test_dbname_upd "rlocal" || exit 1

}

main 2>&1 | tee -a `basename $0`_`date "+%Y%m%d_%H%M%S"`.log

echo
echo "test completed `date`"
