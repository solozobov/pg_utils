\set QUIET on

-- TODO:
--   look at n_live_tup, n_dead_tup, n_tup_hot_upd at https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES-VIEW
--   make :index_gc and :index_permissions
--   look at https://github.com/powa-team/pg_stat_kcache/ and https://github.com/percona/pg_stat_monitor features

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
  INNER JOIN get_heap_bloat_info_bece342fadda() b ON t.schemaname = b.schemaname AND t.relname = b.tblname
  INNER JOIN pg_statio_all_tables io ON t.relid = io.relid
  WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  ORDER BY pg_total_relation_size(t.relid) DESC;
$$ AS table_stats \gset

-- :table_privileges
SELECT $$
  SELECT
    schemaname || '.' || tablename AS "table",
    array_to_string(array_agg(role_name || CASE WHEN as_owner THEN ' ♛' WHEN grantable THEN ' ★' ELSE '' END ORDER BY as_owner DESC, role_name ASC), ', ') AS "role (♛ = owner, ★ = with grant)",
    array_to_string(privilege, ',') AS "privileges"
  FROM (
    SELECT
      n.nspname AS schemaname,
      ' ' || CASE
        WHEN a.defaclobjtype = 'r' THEN 'relations'
        WHEN a.defaclobjtype = 'S' THEN 'sequences'
        WHEN a.defaclobjtype = 'f' THEN 'functions'
        WHEN a.defaclobjtype = 'T' THEN 'types'
        WHEN a.defaclobjtype = 'n' THEN 'schemas'
        ELSE '???'
      END || ' DEFAULTS' AS tablename,
      r.rolname AS role_name,
      false AS as_owner,
      acl.is_grantable AS grantable,
      CASE
        WHEN a.defaclobjtype = 'r' THEN (CASE WHEN array_agg(acl.privilege_type) @> '{"INSERT","SELECT","UPDATE","DELETE","TRUNCATE","REFERENCES","TRIGGER"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT acl.privilege_type ORDER BY acl.privilege_type) END)
        WHEN a.defaclobjtype = 'S' THEN (CASE WHEN array_agg(acl.privilege_type) @> '{"SELECT","UPDATE","USAGE"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT acl.privilege_type ORDER BY acl.privilege_type) END)
        WHEN a.defaclobjtype = 'f' THEN (CASE WHEN array_agg(acl.privilege_type) @> '{"EXECUTE"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT acl.privilege_type ORDER BY acl.privilege_type) END)
        WHEN a.defaclobjtype = 'T' THEN (CASE WHEN array_agg(acl.privilege_type) @> '{"USAGE"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT acl.privilege_type ORDER BY acl.privilege_type) END)
        WHEN a.defaclobjtype = 'n' THEN (CASE WHEN array_agg(acl.privilege_type) @> '{"CREATE","USAGE"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT acl.privilege_type ORDER BY acl.privilege_type) END)
        ELSE array_agg(DISTINCT acl.privilege_type ORDER BY acl.privilege_type)
      END AS privilege
    FROM
      pg_default_acl a,
      aclexplode(a.defaclacl) acl,
      pg_namespace n,
      pg_roles r
    WHERE
      a.defaclnamespace = n.oid
      AND acl.grantee = r.oid
    GROUP BY schemaname, a.defaclobjtype, tablename, role_name, as_owner, grantable
    UNION
    SELECT
      t.schemaname,
      t.tablename,
      g.grantee AS role_name,
      false AS as_owner,
      g.is_grantable = 'YES' AS grantable,
      CASE WHEN array_agg(g.privilege_type) @> '{"INSERT","SELECT","UPDATE","DELETE","TRUNCATE","REFERENCES","TRIGGER"}' THEN '{"ALL"}' ELSE array_agg(DISTINCT g.privilege_type ORDER BY g.privilege_type) END AS privilege
    FROM pg_tables AS t INNER JOIN information_schema.role_table_grants AS g ON g.table_schema = t.schemaname AND g.table_name = t.tablename
    WHERE t.schemaname NOT IN ('pg_catalog','information_schema','tmp')
    GROUP BY schemaname, tablename, role_name, as_owner, grantable
    UNION
    SELECT
      schemaname,
      tablename,
      tableowner AS role_name,
      true AS as_owner,
      true AS grantable,
      '{"ALL"}' AS privilege
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog','information_schema','tmp')
  ) AS s
  GROUP BY schemaname, tablename, "privileges"
  ORDER BY schemaname, tablename, "privileges";
$$ AS table_privileges \gset

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
    CASE WHEN table_rows = 0 THEN NULL ELSE round(((index_rows * 100) / table_rows))::numeric || '%' END AS "row_coverage",
    CASE WHEN indisunique THEN 'Y' ELSE '' END AS unique,
    idx_scan AS index_scans,
    CASE WHEN idx_tup_read = 0 THEN '0%' ELSE ((idx_tup_read - idx_tup_fetch) * 100 / idx_tup_read)::numeric || '%' END AS index_only_returned_tuples,
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
    pg_size_pretty((shared_blks_hit + shared_blks_read) * 8192 - exec_reads) AS memory_hit,
    pg_size_pretty(exec_reads) AS disk_read,
    pg_size_pretty(exec_writes) AS disk_write,
    round(blk_read_time::numeric, 2) AS blk_read_time,
    round(blk_write_time::numeric, 2) AS blk_write_time,
    round(exec_user_time::numeric, 2) AS user_time,
    round(exec_system_time::numeric, 2) AS system_time
  FROM pg_stat_statements s
  JOIN pg_stat_kcache() k USING (userid, dbid, queryid)
  JOIN pg_database d ON s.dbid = d.oid
  JOIN pg_roles r ON r.oid = userid
  WHERE datname != 'postgres' AND datname NOT LIKE 'template%'
  ORDER BY exec_user_time + exec_system_time DESC
  LIMIT 50;
$$ AS hard_queries \gset

-- :tx
SELECT $$
  SELECT now() - xact_start AS duration, *
  FROM pg_stat_activity
  WHERE datname = current_database() AND state <> 'idle' AND pid <> pg_backend_pid()
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

-- :problems
SELECT $$
  WITH indexes as (
    SELECT
      ct.relnamespace::regnamespace || '.' || ct.relname AS table_,
      ci.relnamespace::regnamespace || '.' || ci.relname AS index_,
      array_agg(a.attname::text ORDER BY array_position(i.indkey, a.attnum) ASC) AS fields
    FROM pg_catalog.pg_index i
      INNER JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
      INNER JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
      INNER JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE ct.relnamespace::regnamespace NOT IN ('pg_catalog', 'pg_toast')
    GROUP BY ct.relnamespace, ct.relname, ci.relnamespace, ci.relname
  ),
  foreign_keys as (
    SELECT
      tc.constraint_schema || '.' || tc.constraint_name AS foreign_key,
      kcu.table_schema || '.' || kcu.table_name AS from_table,
      ARRAY_AGG(DISTINCT kcu.column_name::text) AS from_columns,
      ccu.table_schema || '.' || ccu.table_name AS to_table,
      ARRAY_AGG(DISTINCT ccu.column_name::text) AS to_columns
    FROM information_schema.table_constraints tc
    INNER JOIN information_schema.key_column_usage kcu
      ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name
    INNER JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_catalog = ccu.constraint_catalog
      AND tc.constraint_schema = ccu.constraint_schema
      AND tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY' AND kcu.table_schema NOT IN ('pg_catalog', 'pg_toast')
    GROUP BY tc.constraint_catalog, tc.constraint_schema, tc.constraint_name, kcu.table_schema, kcu.table_name, ccu.table_schema, ccu.table_name
  )
  SELECT 'WARNING' AS severity,
         'Possible full scans of table ''' || fk.from_table || ''' in case of row deletions from table ''' || fk.to_table || '''.' AS problem,
         'Database will do full scans of table ''' || fk.from_table || ''' to delete rows from table ''' || fk.to_table || ''' because of foreign key constraint ''' || fk.foreign_key || '''.' AS description,
         'Add index on table ''' || fk.from_table || ''' columns ''' || ARRAY_TO_STRING(fk.from_columns, ',') || ''' that are declared as foreign key.' AS "possible solution"
  FROM foreign_keys fk LEFT JOIN indexes i ON fk.from_table = i.table_ AND (i.fields)[1:array_length(fk.from_columns, 1)] @> fk.from_columns
  WHERE i.index_ IS NULL;
$$ AS problems \gset

-- :locks
SELECT $$
  SELECT
    l.pid,
    l.virtualtransaction AS tx,
    l.mode AS "lock mode", -- https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-TABLES
    CASE WHEN c.relkind IS NOT NULL THEN
      CASE
        WHEN c.relkind = 'r' THEN 'table'
        WHEN c.relkind = 'i' THEN 'index'
        WHEN c.relkind = 'S' THEN 'sequence'
        WHEN c.relkind = 't' THEN 'TOAST table'
        WHEN c.relkind = 'v' THEN 'view'
        WHEN c.relkind = 'm' THEN 'materialized view'
        WHEN c.relkind = 'c' THEN 'composite'
        WHEN c.relkind = 'f' THEN 'foreign table'
        WHEN c.relkind = 'p' THEN 'partitioned table'
        WHEN c.relkind = 'I' THEN 'partitioned index'
        ELSE '???'
      END || ' ' || c.relnamespace::regnamespace || '.' || c.relname || ' '
      ELSE ''
    END ||
    CASE -- https://www.postgresql.org/docs/17/monitoring-stats.html#WAIT-EVENT-LOCK-TABLE
      WHEN l.locktype IN ('relation', 'page', 'tuple') THEN ''
      WHEN l.locktype = 'advisory' THEN 'advisory '
      WHEN l.locktype = 'transactionid' THEN 'transaction to finish '
      WHEN l.locktype = 'applytransaction' THEN 'remote transaction being applied by a logical replication subscriber '
      WHEN l.locktype = 'spectoken' THEN 'speculative insertion lock '
      WHEN l.locktype = 'frozenid' THEN 'update pg_database.datfrozenxid and pg_database.datminmxid lock '
      WHEN l.locktype = 'extend' THEN 'extend a relation lock '
      WHEN l.locktype = 'object' THEN 'non-relation database object lock '
      WHEN l.locktype = 'virtualxid' THEN 'virtual transaction ID lock '
      WHEN l.locktype = 'userlock' THEN 'user lock '
      ELSE '??? '
    END ||
    CASE WHEN l.virtualxid IS NOT NULL THEN 'virtual transaction' ELSE '' END ||
    CASE WHEN l.transactionid IS NOT NULL THEN 'transaction' ELSE '' END
    AS "lock object",
    CASE WHEN l.locktype = 'advisory' THEN
      CASE
        WHEN l.objsubid = 1 THEN (l.classid::bigint << 32) | l.objid::bigint || ''
        WHEN l.objsubid = 2 THEN '(' || l.classid || ', ' || l.objid || ')'
        ELSE '???'
      END
    ELSE
      CASE WHEN l.classid IS NOT NULL THEN 'pg_class.oid#' || l.classid || ' ' ELSE '' END ||
      CASE WHEN l.virtualxid IS NOT NULL THEN l.virtualxid || ' ' ELSE '' END ||
      CASE WHEN l.transactionid IS NOT NULL THEN l.transactionid || ' ' ELSE '' END ||
      CASE WHEN l.page IS NOT NULL THEN 'page #' || l.page || ' ' ELSE '' END ||
      CASE WHEN l.tuple IS NOT NULL THEN 'tuple ' || l.tuple || ' ' ELSE '' END ||
      CASE WHEN l.objid IS NOT NULL THEN 'object #' || l.objid || ' ' ELSE '' END ||
      CASE WHEN l.objsubid IS NOT NULL THEN 'column #' || l.objsubid || ' ' ELSE '' END
    END
    AS "lock details",
    CASE WHEN l.granted THEN 'acquired' ELSE (CASE WHEN l.waitstart IS NULL THEN '0' ELSE EXTRACT(EPOCH FROM now() - l.waitstart)::int || ' sec' END) END AS "wait time",
    pg_blocking_pids(l.pid) AS "blocked by pids"
  FROM pg_locks l
  LEFT JOIN pg_class c ON l.relation = c.oid
  WHERE c.relnamespace::regnamespace NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  ORDER BY pid, tx;
$$ AS locks \gset

-- :locks_by_objects
SELECT $$
  SELECT
    l.pid,
    l.virtualtransaction AS tx,
    l.mode AS "lock mode", -- https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-TABLES
    CASE -- https://www.postgresql.org/docs/17/monitoring-stats.html#WAIT-EVENT-LOCK-TABLE
      WHEN l.locktype IN ('relation', 'page', 'tuple') THEN ''
      WHEN l.locktype = 'advisory' THEN 'advisory '
      WHEN l.locktype = 'transactionid' THEN 'transaction to finish '
      WHEN l.locktype = 'applytransaction' THEN 'remote transaction being applied by a logical replication subscriber '
      WHEN l.locktype = 'spectoken' THEN 'speculative insertion lock '
      WHEN l.locktype = 'frozenid' THEN 'update pg_database.datfrozenxid and pg_database.datminmxid lock '
      WHEN l.locktype = 'extend' THEN 'extend a relation lock '
      WHEN l.locktype = 'object' THEN 'non-relation database object lock '
      WHEN l.locktype = 'virtualxid' THEN 'virtual transaction ID lock '
      WHEN l.locktype = 'userlock' THEN 'user lock '
      ELSE '??? '
    END ||
    CASE WHEN c.relkind IS NOT NULL THEN
      CASE
        WHEN c.relkind = 'r' THEN 'table'
        WHEN c.relkind = 'i' THEN 'index'
        WHEN c.relkind = 'S' THEN 'sequence'
        WHEN c.relkind = 't' THEN 'TOAST table'
        WHEN c.relkind = 'v' THEN 'view'
        WHEN c.relkind = 'm' THEN 'materialized view'
        WHEN c.relkind = 'c' THEN 'composite'
        WHEN c.relkind = 'f' THEN 'foreign table'
        WHEN c.relkind = 'p' THEN 'partitioned table'
        WHEN c.relkind = 'I' THEN 'partitioned index'
        ELSE '???'
      END || ' ' || c.relnamespace::regnamespace || '.' || c.relname || ' '
      ELSE ''
    END ||
    CASE WHEN l.page IS NOT NULL THEN 'page #' || l.page || ' ' ELSE '' END ||
    CASE WHEN l.tuple IS NOT NULL THEN 'tuple ' || l.tuple || ' ' ELSE '' END ||
    CASE WHEN l.virtualxid IS NOT NULL THEN 'virtual tx ' || l.virtualxid || ' ' ELSE '' END ||
    CASE WHEN l.transactionid IS NOT NULL THEN 'tx ' || l.transactionid || ' ' ELSE '' END ||
    CASE WHEN l.classid IS NOT NULL THEN 'pg_class.oid#' || l.classid || ' ' ELSE '' END ||
    CASE WHEN l.objid IS NOT NULL THEN 'object #' || l.objid || ' ' ELSE '' END ||
    CASE WHEN l.objsubid IS NOT NULL THEN 'column #' || l.objsubid || ' ' ELSE '' END
    AS "lock on object",
    CASE WHEN l.granted THEN 'acquired' ELSE (CASE WHEN l.waitstart IS NULL THEN '0' ELSE EXTRACT(EPOCH FROM now() - l.waitstart)::int || ' sec' END) END AS "wait time"
  FROM pg_locks l
  LEFT JOIN pg_class c ON l.relation = c.oid
  WHERE c.relnamespace::regnamespace NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  GROUP BY l.pid, l.virtualtransaction, l.granted;

$$ AS locks_by_objects \gset

\set QUIET off

\echo 'Functions :table_stats :table_privileges :index_stats :hard_queries :tx :gc :problems added'
