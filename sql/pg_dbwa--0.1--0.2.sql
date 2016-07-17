-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_dbwa" to load this file. \quit


-- Function: dbwa.show_top_queryes(
--     CHARACTER VARYING,
--     TIMESTAMP WITHOUT TIME ZONE,
--     TIMESTAMP WITHOUT TIME ZONE,
--     BIGINT)

CREATE OR REPLACE FUNCTION dbwa.show_top_queryes(
        p_clustername CHARACTER VARYING,
        p_begin_time TIMESTAMP WITHOUT TIME ZONE,
        p_end_time TIMESTAMP WITHOUT TIME ZONE,
        p_limit BIGINT)
    RETURNS table(
        clusterid BIGINT,
        userid BIGINT,
        dbid BIGINT,
        queryid BIGINT,
        first_time TIMESTAMP WITHOUT TIME ZONE,
        last_time TIMESTAMP WITHOUT TIME ZONE,
        username NAME,
        dbname NAME,
        query_text TEXT,
        calls NUMERIC,
        calls_pct NUMERIC,
        total_time DOUBLE PRECISION,
        total_time_pct DOUBLE PRECISION,
        blk_rw_time DOUBLE PRECISION,
        blk_rw_time_pct DOUBLE PRECISION
    ) AS
$body$
DECLARE

/*
Получение топа запросов за указанный период времени

p_clustername   Имя базы данных dbwa.cluster_config.clustername
p_begin_time    Начало периода
p_end_time      Конец периода
p_limit         Кол-во записей, возвращаемых функцией(LIMIT в запросе)
*/

    v_clusterid BIGINT;

BEGIN

    SELECT cc.clusterid INTO v_clusterid FROM dbwa.cluster_config cc
    WHERE cc.clustername = p_clustername;

    RETURN QUERY
    SELECT
        sh_sum.clusterid AS clusterid,
        sh_sum.userid AS userid,
        sh_sum.dbid AS dbid,
        sh_sum.queryid AS queryid,
        ss.first_statop_time AS first_time,
        ss.last_statop_time AS last_time,
        ss.username AS username,
        ss.dbname AS dbname,
        ss.query_text AS query_text,
        sh_sum.calls AS calls,
        trunc(sh_sum.calls*100/sum(sh_sum.calls) OVER (), 2)
            AS calls_pct,
        sh_sum.total_time AS total_time,
        trunc(sh_sum.total_time*10000/sum(sh_sum.total_time) OVER ())/100
            AS total_time_pct,
        sh_sum.blk_rw_time AS blk_rw_time,
        CASE
            WHEN sum(sh_sum.blk_rw_time) OVER () != 0 THEN
                trunc((sh_sum.blk_rw_time)*
                    10000/sum(sh_sum.blk_rw_time) OVER ())/100
            ELSE 0
        END AS blk_rw_time_pct
    FROM (
        SELECT
            sh_diff.clusterid AS clusterid,
            sh_diff.userid AS userid,
            sh_diff.dbid AS dbid,
            sh_diff.queryid AS queryid,
            sum(sh_diff.calls) AS calls,
            sum(sh_diff.total_time) AS total_time,
            sum(sh_diff.blk_rw_time) AS blk_rw_time
        FROM (
            SELECT
                sh.statopid, sh.statop_time,
                sh.clusterid, sh.userid, sh.dbid, sh.queryid,
                sh.calls - lag(sh.calls,1,'0') OVER w AS calls,
                sh.total_time - lag(sh.total_time,1,'0') OVER w AS total_time,
                sh.blk_read_time - lag(sh.blk_read_time,1,'0') OVER w +
                    sh.blk_write_time - lag(sh.blk_write_time,1,'0') OVER w
                    AS blk_rw_time,
                min(statopid) OVER w AS statopid_min
            FROM (SELECT sh0.*
                FROM dbwa.stat_statements_history sh0
                WHERE sh0.clusterid = v_clusterid
                    AND sh0.statop_time
                        BETWEEN p_begin_time
                        AND p_end_time
                ) sh
            WINDOW w AS (
                PARTITION BY sh.clusterid, sh.userid, sh.dbid, sh.queryid
                ORDER BY sh.statopid
                )
            ) sh_diff
        WHERE sh_diff.calls != 0 AND sh_diff.statopid != sh_diff.statopid_min
        GROUP BY sh_diff.clusterid, sh_diff.userid, sh_diff.dbid,
            sh_diff.queryid
        ) sh_sum
    LEFT JOIN dbwa.stat_statements ss
        ON ss.clusterid = v_clusterid
            AND ss.clusterid = sh_sum.clusterid
            AND ss.userid = sh_sum.userid
            AND ss.dbid = sh_sum.dbid
            AND ss.queryid = sh_sum.queryid
    ORDER BY sh_sum.total_time DESC LIMIT p_limit;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION dbwa.show_top_queryes(CHARACTER VARYING,
    TIMESTAMP WITHOUT TIME ZONE, TIMESTAMP WITHOUT TIME ZONE, BIGINT)
    OWNER TO postgres;


-- Function: dbwa.show_stat_query(
--     CHARACTER VARYING,
--     BIGINT,
--     BIGINT,
--     BIGINT,
--     TIMESTAMP WITHOUT TIME ZONE,
--     TIMESTAMP WITHOUT TIME ZONE)

CREATE OR REPLACE FUNCTION dbwa.show_stat_query(
        p_clustername CHARACTER VARYING,
        p_userid BIGINT,
        p_dbid BIGINT,
        p_queryid BIGINT,
        p_begin_time TIMESTAMP WITHOUT TIME ZONE,
        p_end_time TIMESTAMP WITHOUT TIME ZONE)
    RETURNS table(
        clusterid BIGINT,
        userid BIGINT,
        dbid BIGINT,
        queryid BIGINT,
        prev_time TIMESTAMP WITHOUT TIME ZONE,
        curr_time TIMESTAMP WITHOUT TIME ZONE,
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
    ) AS
$body$
DECLARE

/*
Получение статистики выполнения запроса за указанный период времени

p_clustername   Имя базы данных dbwa.cluster_config.clustername
p_userid        id пользователя dbwa.stat_statements.userid
p_dbid          id логической базы данных dbwa.stat_statements.dbid
p_queryid       id запроса dbwa.stat_statements.queryid
p_begin_time    Начало периода
p_end_time      Конец периода
*/

    v_clusterid BIGINT;

BEGIN

    SELECT cc.clusterid INTO v_clusterid FROM dbwa.cluster_config cc
    WHERE cc.clustername = p_clustername;

    RETURN QUERY
    SELECT
        sh_diff.clusterid AS clusterid,
        sh_diff.userid AS userid,
        sh_diff.dbid AS dbid,
        sh_diff.queryid AS queryid,
        sh_diff.prev_statop_time AS prev_time,
        sh_diff.curr_statop_time AS curr_time,
        sh_diff.calls AS calls,
        sh_diff.total_time AS total_time,
        sh_diff.rows AS rows,
        sh_diff.shared_blks_hit AS shared_blks_hit,
        sh_diff.shared_blks_read AS shared_blks_read,
        sh_diff.shared_blks_dirtied AS shared_blks_dirtied,
        sh_diff.shared_blks_written AS shared_blks_written,
        sh_diff.local_blks_hit AS local_blks_hit,
        sh_diff.local_blks_read AS local_blks_read,
        sh_diff.local_blks_dirtied AS local_blks_dirtied,
        sh_diff.local_blks_written AS local_blks_written,
        sh_diff.temp_blks_read AS temp_blks_read,
        sh_diff.temp_blks_written AS temp_blks_written,
        sh_diff.blk_read_time AS blk_read_time,
        sh_diff.blk_write_time AS blk_write_time
    FROM (
        SELECT
            sh.statopid,
            lag(sh.statop_time,1,sh.statop_time) OVER w AS prev_statop_time,
            sh.statop_time AS curr_statop_time,
            sh.clusterid AS clusterid,
            sh.userid AS userid,
            sh.dbid AS dbid,
            sh.queryid AS queryid,
            sh.calls - lag(sh.calls,1,'0') OVER w AS calls,
            sh.total_time - lag(sh.total_time,1,'0') OVER w AS total_time,
            sh.rows - lag(sh.rows,1,'0') OVER w AS rows,
            sh.shared_blks_hit - lag(sh.shared_blks_hit,1,'0') OVER w
                AS shared_blks_hit,
            sh.shared_blks_read - lag(sh.shared_blks_read,1,'0') OVER w
                AS shared_blks_read,
            sh.shared_blks_dirtied - lag(sh.shared_blks_dirtied,1,'0') OVER w
                AS shared_blks_dirtied,
            sh.shared_blks_written - lag(sh.shared_blks_written,1,'0') OVER w
                AS shared_blks_written,
            sh.local_blks_hit - lag(sh.local_blks_hit,1,'0') OVER w
                AS local_blks_hit,
            sh.local_blks_read - lag(sh.local_blks_read,1,'0') OVER w
                AS local_blks_read,
            sh.local_blks_dirtied - lag(sh.local_blks_dirtied,1,'0') OVER w
                AS local_blks_dirtied,
            sh.local_blks_written - lag(sh.local_blks_written,1,'0') OVER w
                AS local_blks_written,
            sh.temp_blks_read - lag(sh.temp_blks_read,1,'0') OVER w
                AS temp_blks_read,
            sh.temp_blks_written - lag(sh.temp_blks_written,1,'0') OVER w
                AS temp_blks_written,
            sh.blk_read_time - lag(sh.blk_read_time,1,'0') OVER w
                AS blk_read_time,
            sh.blk_write_time - lag(sh.blk_write_time,1,'0') OVER w
                AS blk_write_time,
            min(statopid) OVER w AS statopid_min
        FROM (SELECT sh0.*
            FROM dbwa.stat_statements_history sh0
            WHERE sh0.clusterid = v_clusterid
                AND sh0.userid = p_userid
                AND sh0.dbid = p_dbid
                AND sh0.queryid = p_queryid
                AND sh0.statop_time
                    BETWEEN p_begin_time
                    AND p_end_time
            ) sh
        WINDOW w AS (
            PARTITION BY sh.clusterid, sh.userid, sh.dbid, sh.queryid
            ORDER BY sh.statopid
            )
        ) sh_diff
    WHERE sh_diff.calls != 0 AND sh_diff.statopid != sh_diff.statopid_min;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION dbwa.show_stat_query(CHARACTER VARYING, BIGINT, BIGINT, BIGINT,
    TIMESTAMP WITHOUT TIME ZONE, TIMESTAMP WITHOUT TIME ZONE)
    OWNER TO postgres;

