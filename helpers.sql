\set QUIET on

-- :table_stats
SELECT $$
  SELECT t.schemaname || '.' || t.relname AS table,
         pg_size_pretty(pg_total_relation_size(t.relid)) AS total_size,
         pg_size_pretty(pg_table_size(t.relid)) AS table_size,
         pg_size_pretty(pg_indexes_size(t.relid)) AS indexes_size,
         round(b.bloat_pct::numeric) || '%' AS mvcc_garbage_rows,
         CASE WHEN seq_scan + idx_scan = 0 THEN '0%' ELSE (seq_scan * 100 / (seq_scan + idx_scan)):: numeric  || '%' END AS seq_scans,
         CASE WHEN heap_blks_hit + heap_blks_read = 0 THEN '0%' ELSE (heap_blks_read * 100 / (heap_blks_hit + heap_blks_read))::numeric || '%' END AS "pages_red_from_disc",
         c.reltuples::numeric AS rows,
         n_tup_ins AS inserts,
         n_tup_upd AS updates,
         n_tup_del AS deletes
  FROM pg_stat_all_tables t
  INNER JOIN pg_class c ON t.relid = c.oid
  INNER JOIN pg_temp.get_heap_bloat_info() b ON t.schemaname = b.schemaname AND t.relname = b.tblname
  INNER JOIN pg_statio_all_tables io ON t.relid = io.relid
  WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  ORDER BY pg_total_relation_size(t.relid) DESC;
$$ AS table_stats \gset

-- :table_permissions
SELECT $$
  SELECT
    schemaname || '.' || tablename AS "table",
    array_to_string(array_agg(role_name || CASE WHEN grantable THEN ' ♛' ELSE '' END ORDER BY role_name), ', ') AS "role (♛ = with grant)",
    array_to_string(permissions, ',') AS permissions
  FROM (
    SELECT
      CASE WHEN g.table_schema IS NULL THEN t.schemaname ELSE g.table_schema         END AS schemaname,
      CASE WHEN g.table_name   IS NULL THEN t.tablename  ELSE g.table_name           END AS tablename,
      CASE WHEN g.grantee      IS NULL THEN t.tableowner ELSE g.grantee              END AS role_name,
      CASE WHEN g.is_grantable IS NULL THEN true         ELSE g.is_grantable = 'YES' END AS grantable,
      CASE WHEN bool_or(g.privilege_type IS NULL) OR array_agg(g.privilege_type) @> '{"INSERT","SELECT","UPDATE","DELETE","TRUNCATE","REFERENCES","TRIGGER"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT g.privilege_type ORDER BY g.privilege_type) END AS permissions
    FROM information_schema.role_table_grants AS g FULL JOIN pg_tables AS t ON g.table_schema = t.schemaname AND g.table_name = t.tablename AND g.grantee = t.tableowner
    WHERE (g.table_schema IS NULL OR g.table_schema NOT IN ('pg_catalog','information_schema','tmp')) AND (t.schemaname IS NULL OR t.schemaname NOT IN ('pg_catalog','information_schema','tmp'))
    GROUP BY
      CASE WHEN g.table_schema IS NULL THEN t.schemaname ELSE g.table_schema         END,
      CASE WHEN g.table_name   IS NULL THEN t.tablename  ELSE g.table_name           END,
      CASE WHEN g.grantee      IS NULL THEN t.tableowner ELSE g.grantee              END,
      CASE WHEN g.is_grantable IS NULL THEN true         ELSE g.is_grantable = 'YES' END
  ) AS s
  GROUP BY schemaname, tablename, permissions
  ORDER BY schemaname, tablename, permissions;
$$ AS table_permissions \gset

-- :index_stats
SELECT $$
  WITH
    duplicates AS (
      SELECT row_number() OVER () AS index, array_agg(indexrelid) AS tables
      FROM pg_index
      WHERE indimmediate
        AND NOT indisexclusion
        AND NOT indisclustered
        AND NOT indcheckxmin
        AND NOT indisreplident
        AND 0 < ALL(indkey)
      GROUP BY (indrelid, indclass, indkey, indoption, indcollation, pg_get_expr(indexprs, indrelid), pg_get_expr(indpred, indrelid))
      HAVING COUNT(*) > 1
    ),
    r AS (
      SELECT
        s.*,
        i.indrelid,
        i.indisunique,
        i.indisvalid,
        i.indisready,
        d.index AS duplicate_index,
        io.idx_blks_read,
        io.idx_blks_hit,
        ct.reltuples table_rows,
        ci.reltuples index_rows,
        pg_relation_size(i.indexrelid) AS index_size,
        pg_relation_size(i.indrelid) AS table_size
      FROM pg_index i
      INNER JOIN pg_stat_all_indexes s ON i.indexrelid = s.indexrelid
      INNER JOIN pg_statio_all_indexes io ON i.indexrelid = io.indexrelid
      INNER JOIN pg_class ct ON ct.oid = i.indrelid
      INNER JOIN pg_class ci ON ci.oid = i.indexrelid
      LEFT JOIN duplicates d ON i.indexrelid = ANY(d.tables)
      WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    )
  SELECT
    schemaname || '.' || indexrelname || CASE WHEN r.indisvalid THEN '' ELSE ' INVALID' END || CASE WHEN r.indisready THEN '' ELSE ' DISABLED' END || CASE WHEN r.duplicate_index ISNULL THEN '' ELSE ' DUPn' || r.duplicate_index END AS index,
    -- r.indrelid::regclass AS table,
    pg_size_pretty(index_size) AS size,
    CASE WHEN table_size = 0 THEN NULL ELSE ((index_size * 100) / table_size)::numeric || '%' END AS "%_of_table_size",
    CASE WHEN table_rows = 0 THEN NULL ELSE round(((index_rows * 100) / table_rows))::numeric || '%' END AS "%_row_coverage",
    CASE WHEN indisunique THEN 'Y' ELSE '' END AS unique,
    idx_scan AS index_scans,
    CASE WHEN idx_tup_read + idx_tup_fetch = 0 THEN '0%' ELSE (idx_tup_fetch * 100 / (idx_tup_read + idx_tup_fetch))::numeric || '%' END AS tuples_red_from_table,
    CASE WHEN idx_blks_hit + idx_blks_read = 0 THEN '0%' ELSE (idx_blks_read * 100 / (idx_blks_hit + idx_blks_read))::numeric || '%' END AS pages_red_from_disc
  FROM r
  ORDER BY index_size DESC;
$$ AS index_stats \gset

-- :hard_queries
SELECT $$
  SELECT
    rolname,
    substring(query, 0, 200),
    round(total_exec_time::numeric, 2) AS execution_time,
    calls,
    pg_size_pretty((shared_blks_hit + shared_blks_read) * 8192 - reads) AS memory_hit,
    pg_size_pretty(reads) AS disk_read,
    pg_size_pretty(writes) AS disk_write,
    round(blk_read_time::numeric, 2) AS blk_read_time,
    round(blk_write_time::numeric, 2) AS blk_write_time,
    round(user_time::numeric, 2) AS user_time,
    round(system_time::numeric, 2) AS system_time
  FROM pg_stat_statements s
  JOIN pg_stat_kcache() k USING (userid, dbid, queryid)
  JOIN pg_database d ON s.dbid = d.oid
  JOIN pg_roles r ON r.oid = userid
  WHERE datname != 'postgres' AND datname NOT LIKE 'template%'
  ORDER BY user_time DESC
  LIMIT 50;
$$ AS hard_queries \gset

-- :tx
SELECT $$
  SELECT now() - xact_start AS duration, *
  FROM pg_stat_activity
  WHERE datname = current_database() AND state <> 'idle' AND pid <> pg_backend_pid() AND query not like 'autovacuum:%'
  ORDER BY now() - xact_start DESC;
$$ AS tx \gset

-- :gc
SELECT $$
  SELECT
    schemaname || '.' || relname AS table,
    pg_size_pretty(pg_total_relation_size(relid)) AS size,
    n_live_tup + n_dead_tup AS tuples,
    round(n_dead_tup::float8 / (n_live_tup + n_dead_tup + 1) * 100) AS "garbage_%",
    last_autovacuum,
    last_autoanalyze
  FROM pg_stat_all_tables
  WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  ORDER BY n_dead_tup / (n_live_tup * current_setting('autovacuum_vacuum_scale_factor')::float8 + current_setting('autovacuum_vacuum_threshold')::float8) DESC
  LIMIT 30;
$$ AS gc \gset

\set QUIET off

\echo 'Functions :table_stats :table_permissions :index_stats :hard_queries :gc added'
