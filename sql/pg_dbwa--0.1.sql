-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_dbwa" to load this file. \quit


-- Sequence: dbwa.clusterid_seq

CREATE SEQUENCE dbwa.clusterid_seq
    INCREMENT 1
    MINVALUE 0
    MAXVALUE 999999999999999
    START 1
    CACHE 1;

ALTER TABLE dbwa.clusterid_seq OWNER TO postgres;


-- Table: dbwa.cluster_config

CREATE TABLE dbwa.cluster_config
(
    clusterid BIGINT NOT NULL DEFAULT nextval('dbwa.clusterid_seq'),
    clustername CHARACTER VARYING(30) NOT NULL,
    rhost CHARACTER VARYING(30) NOT NULL DEFAULT 'localhost',
    rport CHARACTER VARYING(4) NOT NULL DEFAULT '5432',
    rdbname CHARACTER VARYING(30) NOT NULL DEFAULT 'dbname',
    rusername CHARACTER VARYING(30) NOT NULL DEFAULT 'username',
    rpassword CHARACTER VARYING(30) NOT NULL DEFAULT 'password',
    enabled CHARACTER VARYING(1) NOT NULL DEFAULT 'N',
    CONSTRAINT cluster_config_pk PRIMARY KEY (clusterid),
    CONSTRAINT cluster_config_un01 UNIQUE (clustername),
    CONSTRAINT cluster_config_un02 UNIQUE (rhost, rport)
)
WITH (
    OIDS=FALSE
);

ALTER TABLE dbwa.cluster_config OWNER TO postgres;


-- Sequence: dbwa.opid_seq

CREATE SEQUENCE dbwa.opid_seq
    INCREMENT 1
    MINVALUE 0
    MAXVALUE 999999999999999
    START 1
    CACHE 1;

ALTER TABLE dbwa.opid_seq OWNER TO postgres;


-- Table: dbwa.cluster_operation

CREATE TABLE dbwa.cluster_operation
(
    opid BIGINT NOT NULL DEFAULT nextval('dbwa.opid_seq'),
    clusterid BIGINT NOT NULL,
    operation CHARACTER VARYING(250) NOT NULL,
    statopid BIGINT,
    statop_time TIMESTAMP WITHOUT TIME ZONE,
    statop_status CHARACTER VARYING(1),
    enabled CHARACTER VARYING(1),
    CONSTRAINT cluster_operation_pk PRIMARY KEY (opid),
    CONSTRAINT cluster_operation_fk01 FOREIGN KEY (clusterid)
        REFERENCES dbwa.cluster_config (clusterid) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
    OIDS=FALSE
);

ALTER TABLE dbwa.cluster_operation OWNER TO postgres;

-- Index: dbwa.fki_cluster_operation_fk01

CREATE INDEX fki_cluster_operation_fk01
    ON dbwa.cluster_operation
    USING btree
    (clusterid);


-- Sequence: dbwa.statid_seq

CREATE SEQUENCE dbwa.statid_seq
    INCREMENT 1
    MINVALUE 0
    MAXVALUE 999999999999999
    START 1
    CACHE 1;

ALTER TABLE dbwa.statid_seq OWNER TO postgres;


-- Table: dbwa.stat_history

CREATE TABLE dbwa.stat_history
(
    statid BIGINT NOT NULL DEFAULT nextval('dbwa.statid_seq'),
    clusterid BIGINT NOT NULL,
    begin_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    stat_time TIMESTAMP WITHOUT TIME ZONE,
    stat_status CHARACTER VARYING(1),
    stat_info TEXT,
    CONSTRAINT stat_history_pk PRIMARY KEY (statid)
)
WITH (
    OIDS=FALSE
);

ALTER TABLE dbwa.stat_history OWNER TO postgres;

SELECT * FROM prttn_tools.part_time_create_trigger(
    'dbwa',
    'stat_history',
    'stat_time',
    'month',
    TRUE
);


-- Sequence: dbwa.statopid_seq

CREATE SEQUENCE dbwa.statopid_seq
    INCREMENT 1
    MINVALUE 0
    MAXVALUE 999999999999999
    START 1
    CACHE 1;

ALTER TABLE dbwa.statopid_seq OWNER TO postgres;


-- Table: dbwa.statop_history

CREATE TABLE dbwa.statop_history
(
    statopid BIGINT NOT NULL DEFAULT nextval('dbwa.statopid_seq'),
    statid BIGINT NOT NULL,
    clusterid BIGINT NOT NULL,
    opid BIGINT NOT NULL,
    begin_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    statop_time TIMESTAMP WITHOUT TIME ZONE,
    statop_status CHARACTER VARYING(1),
    statop_info TEXT,
    CONSTRAINT statop_history_pk PRIMARY KEY (statopid)
)
WITH (
    OIDS=FALSE
);

ALTER TABLE dbwa.statop_history OWNER TO postgres;

SELECT * FROM prttn_tools.part_time_create_trigger(
    'dbwa',
    'statop_history',
    'statop_time',
    'month',
    TRUE
);


-- Function: dbwa.get_stat(character varying)

CREATE OR REPLACE FUNCTION dbwa.get_stat(p_clustername CHARACTER VARYING)
    RETURNS TEXT AS
$body$
DECLARE

    v_statid dbwa.stat_history.statid%TYPE;
    v_stat_info dbwa.stat_history.stat_info%TYPE;
    v_statopid dbwa.statop_history.statopid%TYPE;

    v_clusterid dbwa.cluster_config.clusterid%TYPE;
    v_clustername dbwa.cluster_config.clustername%TYPE;
    v_rhost dbwa.cluster_config.rhost%TYPE;
    v_rport dbwa.cluster_config.rport%TYPE;
    v_rdbname dbwa.cluster_config.rdbname%TYPE;
    v_rusername dbwa.cluster_config.rusername%TYPE;
    v_rpassword dbwa.cluster_config.rpassword%TYPE;

    v_count BIGINT; 
    v_remote_db_connect CHARACTER VARYING(250);
    r RECORD;
    v_result CHARACTER VARYING(250);
    v_status TEXT;

BEGIN

    v_status := 'ok';
  
    v_clustername := p_clustername;

    SELECT nextval('dbwa.statid_seq') INTO v_statid;

    -- Проверка существования конфигурации для clustername
    SELECT count(*) INTO v_count
    FROM dbwa.cluster_config
    WHERE clustername = v_clustername;
    IF v_count = 0 THEN
        RAISE 'Definition not found for clustername=%', v_clustername;
    END IF;

    -- Получение параметров подключения
    SELECT clusterid, rhost, rport, rdbname, rusername, rpassword
        INTO v_clusterid, v_rhost, v_rport, v_rdbname, v_rusername,
            v_rpassword
    FROM dbwa.cluster_config
    WHERE clustername = v_clustername;

    -- Добавление информации о запуске
    INSERT INTO dbwa.stat_history (statid, clusterid, begin_time,
        stat_time, stat_status)
    VALUES(v_statid, v_clusterid, statement_timestamp(), now(), 'R');

    BEGIN

        -- Соединение с удаленной базой данных
        IF v_clustername != 'local' THEN
            v_remote_db_connect := 'host='||v_rhost||
                ' port='||v_rport||
                ' dbname='||v_rdbname||
                ' user='||v_rusername||
                ' password='||v_rpassword;
            SELECT dblink_connect('remotedb',v_remote_db_connect)
                INTO v_result;
        END IF;

        -- выполнение операций, заданных в cluster_operation
        FOR r IN
            SELECT * FROM dbwa.cluster_operation
            WHERE clusterid = v_clusterid AND enabled = 'Y'
        LOOP

            -- Добавление информации о начале операции
            v_statopid := nextval('dbwa.statopid_seq');
            INSERT INTO dbwa.statop_history (statopid, statid,
                clusterid, opid, begin_time, statop_time, statop_status)
            VALUES(v_statopid, v_statid, v_clusterid, r.opid,
                statement_timestamp(), now(), 'R');

            -- Обновление информации о операции в dbwa.cluster_operation
            UPDATE dbwa.cluster_operation
            SET statopid = v_statopid,
                statop_time = now(),
                statop_status = 'R'
            WHERE opid = r.opid;

            -- Выполнение
            BEGIN

                -- Выполнение операции
                EXECUTE 'SELECT '|| r.operation ||'($1,$2)' INTO v_result
                    USING v_clusterid, v_statopid;

                -- Запись статуса выполнения операции в dbwa.statop_history
                UPDATE dbwa.statop_history
                SET statop_status = 'C',
                    statop_info = 'Сompleted successfully',
                    end_time = statement_timestamp()
                WHERE statopid = v_statopid;

                -- Запись статуса выполнения операции в dbwa.cluster_operation
                UPDATE dbwa.cluster_operation
                SET statopid = v_statopid,
                    statop_time = now(),
                    statop_status = 'C'
                WHERE opid = r.opid;

            EXCEPTION

                WHEN others THEN

                    -- Запись информации об ошибке в dbwa.statop_history
                    UPDATE dbwa.statop_history
                    SET statop_status = 'E',
                        statop_info = SQLSTATE||', '||SQLERRM,
                        end_time = statement_timestamp()
                    WHERE statopid = v_statopid;

                    -- Запись информации об ошибке в dbwa.cluster_operation
                    UPDATE dbwa.cluster_operation
                    SET statopid = v_statopid,
                        statop_time = now(),
                        statop_status = 'E'
                    WHERE opid = r.opid;

                    v_status := 'error';

            END;

        END LOOP;
       
        -- Завершение соединения с удаленной базой данных
        IF v_clustername != 'local' THEN
            SELECT dblink_disconnect('remotedb') INTO v_result;
        END IF;
  
    EXCEPTION

        WHEN others THEN

            -- Запись информации об ошибке в dbwa.stat_history
            UPDATE dbwa.stat_history
            SET stat_status = 'E',
                stat_info = SQLSTATE||', '||SQLERRM,
                end_time = statement_timestamp()
            WHERE statid = v_statid;

            RETURN 'error';

    END;

    IF v_status = 'ok' THEN
        v_stat_info := 'Сompleted successfully';
    ELSE
        v_stat_info := 'Completed with errors';
    END IF;

    -- Запись об успешном выполнении операции в dbwa.stat_history
    UPDATE dbwa.stat_history
    SET stat_status = 'C',
        stat_info = v_stat_info,
        end_time = statement_timestamp()
    WHERE statid = v_statid;

    RETURN v_status;
  
END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION dbwa.get_stat(CHARACTER VARYING)
    OWNER TO postgres;


-- stat statements

-- Table: dbwa.stat_statements

CREATE TABLE dbwa.stat_statements
(
    clusterid BIGINT NOT NULL,
    userid BIGINT NOT NULL,
    dbid BIGINT NOT NULL,
    queryid BIGINT NOT NULL,
    first_statopid BIGINT NOT NULL,
    first_statop_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_statopid BIGINT NOT NULL,
    last_statop_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    calls BIGINT,
    username NAME,
    dbname NAME,
    query_text TEXT NOT NULL,
    CONSTRAINT stat_statements_pk PRIMARY KEY (clusterid, userid, dbid, queryid)
)
WITH (
    OIDS=FALSE
);

ALTER TABLE dbwa.stat_statements OWNER TO postgres;

-- partition dbwa.stat_statements

SELECT * FROM prttn_tools.part_list_create_trigger(
    'dbwa',
    'stat_statements',
    'clusterid',
    TRUE
);


-- stat_statements_history

-- Sequence: dbwa.stat_statements_historyid_seq

CREATE SEQUENCE dbwa.stat_statements_historyid_seq
    INCREMENT 1
    MINVALUE 0
    MAXVALUE 999999999999999
    START 1
    CACHE 1;

ALTER TABLE dbwa.stat_statements_historyid_seq OWNER TO postgres;

-- Table: dbwa.stat_statements_history

CREATE TABLE dbwa.stat_statements_history
(
    historyid BIGINT NOT NULL
        DEFAULT nextval('dbwa.stat_statements_historyid_seq'),
    clusterid BIGINT NOT NULL,
    statopid BIGINT NOT NULL,
    statop_time TIMESTAMP WITHOUT TIME ZONE,
    userid BIGINT NOT NULL,
    dbid BIGINT NOT NULL,
    queryid BIGINT NOT NULL,
    calls BIGINT,
    total_time DOUBLE PRECISION,
    rows BIGINT,
    shared_blks_hit BIGINT,
    shared_blks_read BIGINT,
    shared_blks_dirtied BIGINT,
    shared_blks_written BIGINT,
    local_blks_hit BIGINT,
    local_blks_read BIGINT,
    local_blks_dirtied BIGINT,
    local_blks_written BIGINT,
    temp_blks_read BIGINT,
    temp_blks_written BIGINT,
    blk_read_time DOUBLE PRECISION,
    blk_write_time DOUBLE PRECISION,
    CONSTRAINT stat_statements_history_pk PRIMARY KEY (historyid)
)
WITH (
    OIDS=FALSE
);

ALTER TABLE dbwa.stat_statements_history OWNER TO postgres;

-- Index: dbwa.stat_statements_history_idx01

CREATE INDEX statements_history_statop_time_idx
    ON dbwa.stat_statements_history
    USING btree
    (statop_time);

-- partition dbwa.stat_statements_history

SELECT * FROM prttn_tools.part_list_time_create_trigger(
    'dbwa',
    'stat_statements_history',
    'clusterid',
    'statop_time',
    'day',
    TRUE
);


-- Function: dbwa.get_statements(numeric, numeric)

CREATE OR REPLACE FUNCTION dbwa.get_statements(
    p_clusterid NUMERIC,
    p_statopid NUMERIC)
    RETURNS TEXT AS
$body$
DECLARE

    v_clusterid dbwa.cluster_config.clusterid%TYPE;
    v_statopid dbwa.stat_statements_history.statopid%TYPE;

    v_result CHARACTER VARYING(2);
  
BEGIN

    v_clusterid := p_clusterid;
    v_statopid := p_statopid;

    -- Получение из удаленной базы данных содержимого pg_stat_statements
    -- Обновление статистики в таблицах: stat.statements,
    -- stat.statements_history
    WITH
    pg_stat_statements_tmp AS (
        SELECT
            ss.userid AS userid,
            ss.dbid AS dbid,
            ss.queryid AS queryid,
            coalesce(u.usename,'null') AS username,
            coalesce(d.datname,'null') AS dbname,
            ss.query AS query_text,
            ss.calls,
            ss.total_time,
            ss.rows,
            ss.shared_blks_hit,
            ss.shared_blks_read,
            ss.shared_blks_dirtied,
            ss.shared_blks_written,
            ss.local_blks_hit,
            ss.local_blks_read,
            ss.local_blks_dirtied,
            ss.local_blks_written,
            ss.temp_blks_read,
            ss.temp_blks_written,
            ss.blk_read_time,
            ss.blk_write_time
        FROM eyes.get_pg_stat_statements() ss
        LEFT JOIN pg_user u ON ss.userid= u.usesysid
        LEFT JOIN pg_database d ON ss.dbid = d.oid
    ),
    ins_statements_history AS (
        INSERT INTO dbwa.stat_statements_history
        SELECT nextval('dbwa.stat_statements_historyid_seq'),
            v_clusterid,
            v_statopid,
            now(),
            userid,
            dbid,
            queryid,
            calls,
            total_time,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_dirtied,
            shared_blks_written,
            local_blks_hit,
            local_blks_read,
            local_blks_dirtied,
            local_blks_written,
            temp_blks_read,
            temp_blks_written,
            blk_read_time,
            blk_write_time
        FROM pg_stat_statements_tmp
        ),
        ins_statements AS (
        INSERT INTO dbwa.stat_statements
        SELECT v_clusterid, sst.userid, sst.dbid, sst.queryid, v_statopid,
            now(), v_statopid, now(), sst.calls, sst.username, sst.dbname,
            sst.query_text
        FROM pg_stat_statements_tmp sst
        LEFT JOIN dbwa.stat_statements s ON
            v_clusterid = s.clusterid AND
            sst.userid = s.userid AND
            sst.dbid = s.dbid AND
            sst.queryid = s.queryid
        WHERE s.queryid IS NULL
    ),
    upd_statements AS (
        UPDATE dbwa.stat_statements s
        SET last_statopid = v_statopid,
            last_statop_time = now(),
            calls = sst.calls
        FROM pg_stat_statements_tmp sst
        WHERE s.clusterid = v_clusterid AND
            sst.userid = s.userid AND
            sst.dbid = s.dbid AND
            sst.queryid = s.queryid AND
            s.calls != sst.calls
    )
    SELECT 'ok' INTO v_result;

    RETURN v_result;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION dbwa.get_statements(NUMERIC, NUMERIC)
    OWNER TO postgres;


-- Function: dbwa.get_remote_statements(numeric, numeric)

CREATE OR REPLACE FUNCTION dbwa.get_remote_statements(
    p_clusterid NUMERIC,
    p_statopid NUMERIC)
    RETURNS TEXT AS
$body$
DECLARE

    v_clusterid dbwa.cluster_config.clusterid%TYPE;
    v_statopid dbwa.stat_statements_history.statopid%TYPE;

    v_result CHARACTER VARYING(2);
  
BEGIN

    v_clusterid := p_clusterid;
    v_statopid := p_statopid;

    -- Получение из удаленной базы данных содержимого pg_stat_statements
    -- Обновление статистики в таблицах: stat.statements,
    -- stat.statements_history
    WITH
    pg_stat_statements_tmp AS (
        SELECT *
        FROM dblink('remotedb', 'SELECT
    ss.userid AS userid,
    ss.dbid AS dbid,
    ss.queryid AS queryid,
    coalesce(u.usename,''null'') AS username,
    coalesce(d.datname,''null'') AS dbname,
    query AS query_text,
    calls,
    total_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time
FROM eyes.get_pg_stat_statements() ss
LEFT JOIN pg_user u ON ss.userid= u.usesysid
LEFT JOIN pg_database d ON ss.dbid = d.oid'
            ) AS (
            userid OID,
            dbid OID,
            queryid BIGINT,
            username NAME,
            dbname NAME,
            query_text TEXT,
            calls BIGINT,
            total_time DOUBLE PRECISION,
            rows BIGINT,
            shared_blks_hit BIGINT,
            shared_blks_read BIGINT,
            shared_blks_dirtied BIGINT,
            shared_blks_written BIGINT,
            local_blks_hit BIGINT,
            local_blks_read BIGINT,
            local_blks_dirtied BIGINT,
            local_blks_written BIGINT,
            temp_blks_read BIGINT,
            temp_blks_written BIGINT,
            blk_read_time DOUBLE PRECISION,
            blk_write_time DOUBLE PRECISION
            )
    ),
    ins_statements_history AS (
        INSERT INTO dbwa.stat_statements_history
        SELECT nextval('dbwa.stat_statements_historyid_seq'),
            v_clusterid,
            v_statopid,
            now(),
            userid,
            dbid,
            queryid,
            calls,
            total_time,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_dirtied,
            shared_blks_written,
            local_blks_hit,
            local_blks_read,
            local_blks_dirtied,
            local_blks_written,
            temp_blks_read,
            temp_blks_written,
            blk_read_time,
            blk_write_time
        FROM pg_stat_statements_tmp
        ),
        ins_statements AS (
        INSERT INTO dbwa.stat_statements
        SELECT v_clusterid, sst.userid, sst.dbid, sst.queryid, v_statopid,
            now(), v_statopid, now(), sst.calls, sst.username, sst.dbname,
            sst.query_text
        FROM pg_stat_statements_tmp sst
        LEFT JOIN dbwa.stat_statements s ON
            v_clusterid = s.clusterid AND
            sst.userid = s.userid AND
            sst.dbid = s.dbid AND
            sst.queryid = s.queryid
        WHERE s.queryid IS NULL
    ),
    upd_statements AS (
        UPDATE dbwa.stat_statements s
        SET last_statopid = v_statopid,
            last_statop_time = now(),
            calls = sst.calls
        FROM pg_stat_statements_tmp sst
        WHERE s.clusterid = v_clusterid AND
            sst.userid = s.userid AND
            sst.dbid = s.dbid AND
            sst.queryid = s.queryid AND
            s.calls != sst.calls
    )
    SELECT 'ok' INTO v_result;

    RETURN v_result;

END;
$body$
LANGUAGE plpgsql VOLATILE
COST 100;

ALTER FUNCTION dbwa.get_remote_statements(NUMERIC, NUMERIC)
    OWNER TO postgres;


-- Добавление настроек для локальной БД

DO $$
DECLARE
    v_clusterid dbwa.cluster_config.clusterid%TYPE;
BEGIN

    v_clusterid := nextval('dbwa.clusterid_seq');

    -- Описание базы данных
    INSERT INTO dbwa.cluster_config (clusterid, clustername, enabled)
        VALUES(v_clusterid, 'local', 'Y');
    -- dbwa.getstatements
    INSERT INTO dbwa.cluster_operation (opid, clusterid, operation, enabled)
        VALUES(nextval('dbwa.opid_seq'), v_clusterid, 'dbwa.get_statements',
            'Y');

END $$;

