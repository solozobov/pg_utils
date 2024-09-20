# psql_utils
Set of [`psql`](https://www.postgresql.org/docs/current/app-psql.html) utility functions that helps me analyze PostgreSQL database state.

## Installation
If you want to add these utils to your psql:
1. Checkout this repo anywhere on your computer.
2. Create [`~/.psqlrc`](https://www.postgresql.org/docs/current/app-psql.html#APP-PSQL-FILES-PSQLRC) file in your "home" folder, if it don't exist.
3. Add one line to its beginning `\include >>>path_to__include.sql__file<<<<`
                       
## Usage
After installation `psql` will import utility functions on start and remind you their names with startup message:
```
Functions :table_stats :table_permissions :index_stats :hard_queries :gc :problems added
Function pb(protobuf_message::bytea [, proto_fields_indexes::int]) added
```
You can call any of described functions by typing its name and pressing `Enter` button.

Also command completion works. You can type `:ta` and press `Tab` button, console will complete your input up to `:table_`.
If you press `Tab` button one more time, console will show you available completions `:table_permissions  :table_stats`.

You can disable import of pb() function (parsing Protobuf objects) by commenting out `\include_relative pb_parser.sql` line in `include.sql` file.  

## Available Functions

### :table_stats
Gives information about all available tables. Output example:
```
2024 sep 18 19:31:23 spb-128_f9ecafbe solozobov@eukaryota=> :table_stats
       table       | total_size | table_size | indexes_size | mvcc_garbage_rows | seq_scans | pages_red_from_disc |   rows    |  inserts  | updates | deletes
-------------------+------------+------------+--------------+-------------------+-----------+---------------------+-----------+-----------+---------+---------
 animals.beavers   | 219 GB     | 89 GB      | 129 GB       | 6%                | 95%       | 57%                 | 278721000 |   8848622 |      11 | 5369068
 animals.elephants | 123 GB     | 49 GB      | 74 GB        | 5%                | 0%        | 65%                 | 146143000 | 143110003 | 2554456 |   30633
 plants.roses      | 103 GB     | 72 GB      | 31 GB        | 3%                | 2%        | 41%                 | 280206000 |   4751512 | 2417256 |       0
```
               
`mvcc_garbage_rows` - approximated percent of table rows that may be garbage collected. [See `2. pgsql-bloat-estimation`](#how-it-works).

`seq_scans` - percent of table requests performed with sequential scan of this table. Rest of scans are supposed to use table indexes. [See `seq_tup_read` and `idx_scan`](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES-VIEW).

`pages_red_from_disc` - percent of blocks (file system pages) red from disk. Rest of blocks supposed to be red from special in-memory buffer. [See `heap_blks_read` and `heap_blks_hit`](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-TABLES-VIEW).

`rows` - estimated number of live rows in table. Updated only during VACUUM, ANALYZE, CREATE INDEX and a few other DDL commands. [See `reltuples`](https://www.postgresql.org/docs/current/catalog-pg-class.html#CATALOG-PG-CLASS).  

`inserts`, `updates`, `deletes` - number of inserted, updated and deleted rows in table since last statistics reset. [See `n_tup_ins`, `n_tup_upd` and `n_tup_del`](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES-VIEW). 

### :index_stats
Gives information about all available indexes. Output example:
```
2024 sep 18 19:39:17 spb-128_f9ecafbe solozobov@eukaryota=> :index_stats
                index               |  size   | %_of_table_size | row_coverage | unique | index_scans | index_only_returned_tuples | pages_red_from_disc
------------------------------------+---------+-----------------+--------------+--------+-------------+----------------------------+---------------------
 animals.beavers_pkey               | 59 GB   | 65%             | 100%         | Y      |    93339481 | 49%                        | 80%
 animals.beavers_trees_fell INVALID | 34 GB   | 38%             | 33%          |        |           0 | 0%                         | 0%
 animals.elephants_pkey             | 34 GB   | 69%             | 100%         | Y      |   222919106 | 49%                        | 9%
 animals.elephants_weight DISABLED  | 34 GB   | 69%             | 100%         |        |   222919106 | 49%                        | 9%
 plants.roses_pkey DUPn1            | 17 GB   | 35%             | 100%         | Y      |           0 | 0%                         | 16%
 plants.roses_with_spikes DUPn1     | 20 GB   | 40%             | 50%          |        |           0 | 0%                         | 96%
```
Index names in `index` column sometimes have suffixes:
 - `INVALID` - index was not correctly built (e.g. when `CREATE INDEX CONCURRENTLY` fails) and can't be used. 
 - `DISABLED` - index usage was manually disabled with `update pg_index set indisvalid = false where ...`. This means [index state may by inconsistent with table](https://www.postgresql.org/docs/current/catalog-pg-index.html).
 - `DUPnX` - multiple indexes marked with same `DUPnX` suffix are **potential** duplicates of each other. This mark is given to indexes with same columns in same order, but some of them may be partial, some of them may have more columns than others.

`row_coverage` - estimated percent of table rows covered by index. Helps monitor creation of new indexes and see partial indexes coverage. Updated only during `VACUUM`, `ANALYZE`, `CREATE INDEX` and a few other DDL commands. [See `reltuples`](https://www.postgresql.org/docs/current/catalog-pg-class.html#CATALOG-PG-CLASS).

`index_scans` - number of queries performed with this index. [See `idx_scan`](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-INDEXES-VIEW).

`index_only_returned_tuples` - percent of tuples returned just from index, without getting additional tuple fields from table or toast. Helps you roughly understand how frequently your queries do index_only scans. [See `idx_tup_read` and `idx_tup_fetch`](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ALL-INDEXES-VIEW).

`pages_red_from_disc` - percent of blocks (file system pages) red from disk. Rest of blocks supposed to be red from special in-memory buffer. [See `idx_blks_read` and `idx_blks_hit`](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-INDEXES-VIEW).

### :table_permissions
Gives information about table permissions. Output example:
```
2024 sep 18 19:39:46 spb-128_f9ecafbe solozobov@eukaryota=> :table_permissions
                 table                 |   role (♛ = with grant)      |         permissions
---------------------------------------+------------------------------+-----------------------------
 animals.beavers                       | Alena ♛, Vitaly ♛            | ALL
 animals.elephants                     | Alena ♛                      | SELECT
 animals.elephants                     | Vitaly ♛                     | DELETE,INSERT,SELECT,UPDATE
 plants.roses                          | Alena ♛, Vitaly ♛, Agrippina | ALL
```

### :hard_queries
Shows the most complex and frequently called queries. Reuires `pg_stat_kcache` extension to be installed. Output example:
```
-[ RECORD 1 ]--+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
rolname        | Alena
substring      | SELECT parent, dir, size, key, owner, modtime FROM fs_uploads WHERE path= $1
execution_time | 48465011.63
calls          | 918996370
memory_hit     | 40 TB
disk_read      | 1347 MB
disk_write     | 16 kB
blk_read_time  | 109337.54
blk_write_time | 0.04
user_time      | 63981.77
system_time    | 13591.17
```

### :tx
Shows information about running transactions.

### :gc
Gives information about tables garbage collection state. Output example:
```
2024 sep 18 19:45:46 spb-128_f9ecafbe solozobov@eukaryota=> :gc
       table       |  size   |  tuples   | garbage_% |        last_autovacuum        |       last_autoanalyze
-------------------+---------+-----------+-----------+-------------------------------+-------------------------------
 animals.beavers   | 8792 MB |  12352891 |         2 | 2024-09-18 18:04:52.432419+03 | 2024-09-18 19:01:02.28209+03
 animals.elephants | 133 MB  |    408113 |         0 | 2024-09-17 22:34:42.609921+03 | 2024-09-18 16:51:30.809964+03
 plants.roses      | 132 MB  |    301937 |         0 | 2024-09-16 19:25:45.882332+03 | 2024-09-18 16:54:57.065949+03
```


### :problems
States potential database problems. Output example:

## How it works
Functions combine information from:
1. [PostgreSQL statistics system](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS)
2. [pgsql-bloat-estimation](https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql) bloat function by Jehan-Guillaume (ioguix) de Rorthais
