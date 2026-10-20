-- start_ignore
DROP SCHEMA IF EXISTS orca_srf_subquery_fallback CASCADE;
CREATE SCHEMA orca_srf_subquery_fallback;
SET search_path TO orca_srf_subquery_fallback;
-- end_ignore

-- Test that queries containing a set-returning function (SRF) inside a
-- subquery (in the subquery's target list or quals, including deeper
-- subqueries) fall back from ORCA to the Postgres planner, which plans
-- them correctly. Before this fallback, ORCA produced plans that crashed
-- with internal assertion errors ("Expected no more than one row to be
-- returned by expression") or returned wrong results.

set optimizer=on;

CREATE TABLE srf_t (k int, v int) DISTRIBUTED BY (k);
INSERT INTO srf_t SELECT g, g*10 FROM generate_series(1,10) g;

CREATE TABLE srf_small (k int, c int) DISTRIBUTED BY (k);
INSERT INTO srf_small VALUES (1, 100), (2, 200), (3, 300);

-- SRF in the target list of a subquery qual
explain (costs off)
update srf_t set v = v + 1
where exists (select from (select generate_series(1,3) g) s where s.g = srf_t.k);

-- aggregate over SRF in a subquery qual (agg->SRF->WHERE shape)
explain (costs off)
update srf_t set v = v + 1
where exists (select 1 from (select count(*) over () c, generate_series(1, srf_small.k) g from srf_small) s where s.g > 0);

-- scalar subquery containing an SRF: ORCA used to plan this shape in a way
-- that broke the single-row guarantee of scalar subqueries
explain (costs off)
update srf_t set v = v + 1
where k = (select 1 from (select generate_series(1,3) g) s where s.g > 0);

-- executable variants: the Postgres planner (used after the fallback) must
-- produce the correct results
update srf_t set v = v + 1
where exists (select from (select generate_series(1,3) g) s where s.g = srf_t.k);
select k, v from srf_t order by k;

update srf_t set v = v + 1
where exists (select 1 from (select count(*) over () c, generate_series(1, srf_small.k) g from srf_small) s where s.g > 0);
select k, v from srf_t order by k;

-- SELECT with a correlated SRF inside the subquery target list
select k from srf_t
where exists (select 1 from (select generate_series(1, k) g) s where g = 3)
order by k;

-- deeper nesting: the SRF is two subqueries down
select k from srf_t
where exists (select 1
              from (select * from (select generate_series(1, k) g) s0) s
              where s.g = k)
order by k;

-- SRF inside a CTE: CTEScans are also subqueries, fall back as well
explain (costs off)
select k from srf_t
where k in (with c as (select generate_series(1, 5) g) select g from c)
order by k;

-- control query without an SRF: ORCA still plans it
explain (costs off)
select k from srf_t where exists (select 1 from srf_small where srf_small.k = srf_t.k) order by k;
