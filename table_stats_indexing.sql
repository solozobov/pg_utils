\set QUIET on
SELECT current_setting AS old_client_min_messages FROM current_setting('client_min_messages') \gset
SET client_min_messages TO NOTICE;

\set table_stats_interval_days 30

--\set debug :debug
--DO $$
--BEGIN
--  IF (SELECT CASE WHEN :debug = ':debug' THEN TRUE ELSE FALSE END) THEN
--    \echo 'test defined';
--  END IF;
--END;
--$$;

-- может не быть доступа к pgstattuples
-- может не быть доступа к схеме tmp
-- нужно быть членом mdb_admin
SELECT $$
  CREATE OR REPLACE PROCEDURE pg_temp.init() LANGUAGE PLPGSQL AS $p$
    BEGIN
      BEGIN
        IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'nv_stats') THEN
          CREATE SCHEMA nv_stats;
          GRANT ALL PRIVILEGES ON SCHEMA nv_stats TO mdb_admin;
          GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tmp TO mdb_admin;
          GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tmp TO mdb_admin;
        END IF;

        CREATE OR REPLACE FUNCTION pg_temp.get_table_stats() RETURNS TABLE (
          stats_date DATE,
          table_name TEXT,
          total_size BIGINT,
          table_size BIGINT,
          indexes_size BIGINT,
          garbage DOUBLE PRECISION,
          seq_scan BIGINT,
          idx_scan BIGINT,
          heap_blks_hit BIGINT,
          heap_blks_read BIGINT,
          rows NUMERIC,
          inserts BIGINT,
          updates BIGINT,
          deletes BIGINT
        ) LANGUAGE SQL AS $f$
          SELECT
            CURRENT_DATE,
            t.schemaname || '.' || t.relname,
            pg_total_relation_size(t.relid),
            pg_table_size(t.relid),
            pg_indexes_size(t.relid),
            round((pgstattuple(t.relid)).dead_tuple_percent),
            t.seq_scan,
            t.idx_scan,
            io.heap_blks_hit,
            io.heap_blks_read,
            c.reltuples::numeric AS rows,
            t.n_tup_ins AS inserts,
            t.n_tup_upd AS updates,
            t.n_tup_del AS deletes
          FROM pg_stat_all_tables t
          INNER JOIN pg_class c ON t.relid = c.oid
          INNER JOIN pg_statio_all_tables io ON t.relid = io.relid
          WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
        $f$;

        IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'nv_stats' AND table_name = 'table_stats') THEN
          CREATE TABLE nv_stats.table_stats AS SELECT * FROM pg_temp.get_table_stats();
        END IF;

        IF NOT EXISTS (SELECT 1 FROM nv_stats.table_stats WHERE stats_date = CURRENT_DATE) THEN
          INSERT INTO nv_stats.table_stats SELECT * FROM pg_temp.get_table_stats();
        END IF;

        RAISE NOTICE 'Helper functions:';
        RAISE NOTICE '    :table_stats  - статистика таблиц (требует расширения pgstattuple)';
        RAISE NOTICE '    :index_stats  - статистики индексов';
        RAISE NOTICE '    :hard_queries - статистика самых тяжелых (по CPU или диску) запросов (требует расширений pg_stat_statements и pg_stat_kcache)';
        RAISE NOTICE '    :tx           - список долгих транзакций (выполнять из под нужного пользователя)';
        RAISE NOTICE '    :gc           - доля мусора в таблице и индексах (показатель того, что GC не справляется)';
        RAISE NOTICE '';
      EXCEPTION WHEN others THEN
        RAISE NOTICE 'SORRY: Failed to init helper functions';
      END;
    END
  $p$;
$$ \gexec

CALL pg_temp.init();

-- is_json() -> boolean
SELECT $$
  CREATE OR REPLACE FUNCTION pg_temp.is_json(input_text VARCHAR) RETURNS BOOLEAN LANGUAGE PLPGSQL AS $f$
    DECLARE maybe_json json;
    BEGIN
      BEGIN
        maybe_json := input_text;
      EXCEPTION WHEN others THEN
        RETURN FALSE;
      END;
      RETURN TRUE;
    END
  $f$;
$$ \gexec

-- zip(text[], text[]) -> text[]
-- e.g. SELECT zip(most_common_vals::text::text[], most_common_freqs::text[]) FROM pg_stats WHERE most_common_vals NOTNULL AND schemaname != 'pg_catalog' LIMIT 10;
SELECT $$
  CREATE OR REPLACE FUNCTION pg_temp.zip(a TEXT[], b TEXT[]) RETURNS SETOF TEXT[] LANGUAGE SQL AS $f$
    SELECT ARRAY[x.a,x.b] FROM (SELECT UNNEST(a) AS a, UNNEST(b) AS b) x;
  $f$;
$$ \gexec

SELECT $$
  CREATE OR REPLACE FUNCTION pg_temp.message(msg TEXT) RETURNS TEXT LANGUAGE PLPGSQL AS $f$
    BEGIN
      RAISE NOTICE '%', msg;
      RETURN '';
    END
  $f$;
$$ \gexec

SELECT $$
  CREATE OR REPLACE FUNCTION pg_temp.delta_abs(new NUMERIC, old NUMERIC) RETURNS TEXT LANGUAGE PLPGSQL AS $f$
    BEGIN
      RETURN new || CASE WHEN old = 0 OR ROUND((new - old) * 100 / old) = 0 THEN '' ELSE ' [' || CASE WHEN new > old THEN '+' ELSE '' END || ROUND((new - old) * 100 / old) || '%]' END;
    END
  $f$;
$$ \gexec

SELECT $$
  CREATE OR REPLACE FUNCTION pg_temp.delta_size(new NUMERIC, old NUMERIC) RETURNS TEXT LANGUAGE PLPGSQL AS $f$
    BEGIN
      RETURN pg_size_pretty(new) || CASE WHEN old = 0 OR ROUND((new - old) * 100 / old) = 0 THEN '' ELSE ' [' || CASE WHEN new > old THEN '+' ELSE '' END || ROUND((new - old) * 100 / old) || '%]' END;
    END
  $f$;
$$ \gexec

SELECT $$
  CREATE OR REPLACE FUNCTION pg_temp.delta_perc(new DOUBLE PRECISION, old DOUBLE PRECISION) RETURNS TEXT LANGUAGE PLPGSQL AS $f$
    BEGIN
      RETURN new || '%' || CASE WHEN new = old THEN '' ELSE '[' || CASE WHEN new > old THEN '+' ELSE '' END || new - old || '%]' END;
    END
  $f$;
$$ \gexec

SET client_min_messages TO :old_client_min_messages;
\set QUIET off
