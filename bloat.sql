-- Copyright (c) 2015-2019, Jehan-Guillaume (ioguix) de Rorthais
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are met:
--
-- * Redistributions of source code must retain the above copyright notice, this
--   list of conditions and the following disclaimer.
--
-- * Redistributions in binary form must reproduce the above copyright notice,
--   this list of conditions and the following disclaimer in the documentation
--   and/or other materials provided with the distribution.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
-- AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
-- IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
-- DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
-- FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
-- DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
-- SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
-- CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
-- OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
-- taken from https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql

\set QUIET on

CREATE OR REPLACE FUNCTION pg_temp.get_heap_bloat_info() RETURNS TABLE (
    current_database name,
    schemaname       name,
    tblname          name,
    real_size        numeric,
    extra_size       double precision,
    extra_pct        double precision,
    fillfactor       integer,
    bloat_size       double precision,
    bloat_pct        double precision,
    is_na            boolean
) AS $$ BEGIN RETURN QUERY
    SELECT current_database(), s3.schemaname, s3.tblname, bs*tblpages AS real_size,
      (tblpages-est_tblpages)*bs AS extra_size,
      CASE WHEN tblpages > 0 AND tblpages - est_tblpages > 0
        THEN 100 * (tblpages - est_tblpages)/tblpages::float
        ELSE 0
      END AS extra_pct, s3.fillfactor,
      CASE WHEN tblpages - est_tblpages_ff > 0
        THEN (tblpages-est_tblpages_ff)*bs
        ELSE 0
      END AS bloat_size,
      CASE WHEN tblpages > 0 AND tblpages - est_tblpages_ff > 0
        THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float
        ELSE 0
      END AS bloat_pct, s3.is_na
    FROM (
      SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
        ceil( reltuples / ( (bs-page_hdr)*s2.fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
        tblpages, s2.fillfactor, bs, tblid, s2.schemaname, s2.tblname, heappages, toastpages, s2.is_na
      FROM (
        SELECT
          ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
            - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
            - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
          ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
          toastpages, reltuples, toasttuples, bs, page_hdr, tblid, s.schemaname, s.tblname, s.fillfactor, s.is_na
          -- , tpl_hdr_size, tpl_data_size
        FROM (
          SELECT
            tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
            tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
            coalesce(toast.reltuples, 0) AS toasttuples,
            coalesce(substring(
              array_to_string(tbl.reloptions, ' ')
              FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
            current_setting('block_size')::numeric AS bs,
            CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
            24 AS page_hdr,
            23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN ( 7 + count(s.attname) ) / 8 ELSE 0::int END
               + CASE WHEN bool_or(att.attname = 'oid' and att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size,
            sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 0) ) AS tpl_data_size,
            bool_or(att.atttypid = 'pg_catalog.name'::regtype)
              OR sum(CASE WHEN att.attnum > 0 THEN 1 ELSE 0 END) <> count(s.attname) AS is_na
          FROM pg_attribute AS att
            JOIN pg_class AS tbl ON att.attrelid = tbl.oid
            JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
            LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
              AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
            LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
          WHERE NOT att.attisdropped
            AND tbl.relkind in ('r','m')
          GROUP BY 1,2,3,4,5,6,7,8,9,10
          ORDER BY 2,3
        ) AS s
      ) AS s2
    ) AS s3;
END; $$ LANGUAGE plpgsql;

\set QUIET off
