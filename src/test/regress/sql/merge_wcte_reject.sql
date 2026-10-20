-- start_ignore
DROP SCHEMA IF EXISTS orca_merge_wcte_reject CASCADE;
CREATE SCHEMA orca_merge_wcte_reject;
SET search_path TO orca_merge_wcte_reject;
-- end_ignore

-- Test that MERGE combined with data-modifying CTEs is rejected cleanly
-- (parse-time, ERRCODE_FEATURE_NOT_SUPPORTED) instead of failing with an
-- internal two-writer-gang error or silently dropping the CTE's data
-- modification.

set optimizer=on;

CREATE TABLE m_target (k int, v int) DISTRIBUTED BY (k);
INSERT INTO m_target SELECT g, g*10 FROM generate_series(1,10) g;

CREATE TABLE m_src (k int) DISTRIBUTED BY (k);
INSERT INTO m_src VALUES (1), (2);

-- data-modifying CTEs are rejected under both planners
with c_ins as (insert into m_src values (3) returning 3)
merge into m_target t using m_src s on t.k = s.k
when matched then update set v = v + 1;

with c_upd as (update m_src set k = k returning k)
merge into m_target t using m_src s on t.k = s.k
when matched then update set v = v + 1;

with c_del as (delete from m_src returning k)
merge into m_target t using m_src s on t.k = s.k
when matched then update set v = v + 1;

set optimizer=off;

with c_ins as (insert into m_src values (3) returning 3)
merge into m_target t using m_src s on t.k = s.k
when matched then update set v = v + 1;

-- the rejected statements must not have modified anything
set optimizer=on;
select count(*) from m_src;
select k, v from m_target order by k;

-- plain MERGE keeps working
merge into m_target t using m_src s on t.k = s.k
when matched then update set v = v + 1;
select k, v from m_target order by k;

-- MERGE with a read-only CTE keeps working
with s as (select k from m_src)
merge into m_target t using s on t.k = s.k
when matched then update set v = v + 1;
select k, v from m_target order by k;
