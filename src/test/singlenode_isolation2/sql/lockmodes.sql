-- table to just store the master's data directory path on segment.
CREATE TABLE lockmodes_datadir(a int, dir text);
INSERT INTO lockmodes_datadir select 1,datadir from gp_segment_configuration where role='p' and content=-1;

-- ORCA would upgrade lock to ExclusiveLock
1: set optimizer = on;

create or replace view show_locks_lockmodes as
  select locktype, mode, granted, relation::regclass
  from pg_locks
  where
    gp_segment_id = -1 and
    locktype = 'relation' and
    relation::regclass::text like 't_lockmods%';

show gp_enable_global_deadlock_detector;


-- 1. The firs part of test is with
--    gp_enable_global_deadlock_detector off
--
--    because local deadlock detector can already detect and handle deadlocks in
--    singlenode mode, locking is acted as if GDD is enabled to provide better
--    concurrency, so locks won't be elevated from RowExclusive to Exclusive,
--    thus the following results are different from cluster mode.

-- 1.1 test for heap tables
create table t_lockmods (c int);
insert into t_lockmods select * from generate_series(1, 5);
analyze t_lockmods;

create table t_lockmods1 (c int);

create table t_lockmods_rep(c int);

-- See github issue: https://github.com/greenplum-db/gpdb/issues/9449
-- upsert may lock tuples on segment, so we should upgrade lock level
-- on QD if GDD is disabled.
create table t_lockmods_upsert(a int, b int) ;
create unique index uidx_t_lockmodes_upsert on t_lockmods_upsert(a, b);
-- add analyze to avoid auto vacuum when executing first insert
analyze t_lockmods_upsert;

-- 1.1.1 select for (update|share|key share|no key update) should hold ExclusiveLock on range tables
1: begin;
1: explain select * from t_lockmods for update;
1: select * from t_lockmods for update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods for no key update;
1: select * from t_lockmods for no key update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods for share;
1: select * from t_lockmods for share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods for key share;
1: select * from t_lockmods for key share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods, t_lockmods1 for update;
1: select * from t_lockmods, t_lockmods1 for update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods, t_lockmods1 for no key update;
1: select * from t_lockmods, t_lockmods1 for no key update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods, t_lockmods1 for share;
1: select * from t_lockmods, t_lockmods1 for share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods, t_lockmods1 for key share;
1: select * from t_lockmods, t_lockmods1 for key share;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.1.2 update | delete should hold ExclusiveLock on result relations
1: begin;
1: update t_lockmods set c = c + 0;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: delete from t_lockmods;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.1.3 insert should hold RowExclusiveLock on result relations
1: begin;
1: insert into t_lockmods select * from generate_series(1, 5);
2: select * from show_locks_lockmodes;
1: abort;

-- 1.1.4 upsert should hold ExclusiveLock on result relations
1: begin;
1: insert into t_lockmods_upsert values (1, 1) on conflict(a, b) do update set b = 99;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.1.5 use cached plan should be consistent with no cached plan
1: prepare select_for_update as select * from t_lockmods for update;
1: prepare select_for_nokeyupdate as select * from t_lockmods for no key update;
1: prepare select_for_share as select * from t_lockmods for share;
1: prepare select_for_keyshare as select * from t_lockmods for key share;
1: prepare update_tlockmods as update t_lockmods set c = c + 0;
1: prepare delete_tlockmods as delete from t_lockmods;
1: prepare insert_tlockmods as insert into t_lockmods select * from generate_series(1, 5);
1: prepare upsert_tlockmods as insert into t_lockmods_upsert values (1, 1) on conflict(a, b) do update set b = 99;

1: begin;
1: execute select_for_update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute select_for_nokeyupdate;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute select_for_share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute select_for_keyshare;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute update_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute delete_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute insert_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute upsert_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2 test for AO table
create table t_lockmods_ao (c int) with (appendonly=true);
insert into t_lockmods_ao select * from generate_series(1, 8);
analyze t_lockmods_ao;
create table t_lockmods_ao1 (c int) with (appendonly=true);

-- 1.2.1 select for (update|share|key share|no key update) should hold ExclusiveLock on range tables
1: begin;
1: explain select * from t_lockmods_ao for update;
1: select * from t_lockmods_ao for update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao for no key update;
1: select * from t_lockmods_ao for no key update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao for share;
1: select * from t_lockmods_ao for share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao for key share;
1: select * from t_lockmods_ao for key share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao, t_lockmods_ao1 for update;
1: select * from t_lockmods_ao, t_lockmods_ao1 for update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao, t_lockmods_ao1 for no key update;
1: select * from t_lockmods_ao, t_lockmods_ao1 for no key update;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao, t_lockmods_ao1 for share;
1: select * from t_lockmods_ao, t_lockmods_ao1 for share;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: explain select * from t_lockmods_ao, t_lockmods_ao1 for key share;
1: select * from t_lockmods_ao, t_lockmods_ao1 for key share;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2.2 update | delete should hold ExclusiveLock on result relations
1: begin;
1: update t_lockmods_ao set c = c + 0;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: delete from t_lockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2.3 insert should hold RowExclusiveLock on result relations
1: begin;
1: insert into t_lockmods_ao select * from generate_series(1, 5);
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2.4 use cached plan should be consistent with no cached plan
1: prepare select_for_update_ao as select * from t_lockmods_ao for update;
1: prepare select_for_nokeyupdate_ao as select * from t_lockmods_ao for no key update;
1: prepare select_for_share_ao as select * from t_lockmods_ao for share;
1: prepare select_for_keyshare_ao as select * from t_lockmods_ao for key share;
1: prepare update_tlockmods_ao as update t_lockmods_ao set c = c + 0;
1: prepare delete_tlockmods_ao as delete from t_lockmods_ao;
1: prepare insert_tlockmods_ao as insert into t_lockmods_ao select * from generate_series(1, 5);

1: begin;
1: execute select_for_update_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute select_for_nokeyupdate_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute select_for_share_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute select_for_keyshare_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute update_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute delete_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute insert_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.3 With limit clause, such case should
-- acquire ExclusiveLock on the whole table and do not generate lockrows node
1: begin;
1: explain select * from t_lockmods order by c limit 1 for update;
1: select * from t_lockmods order by c limit 1 for update;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.4 For replicated table, we should lock the entire table on ExclusiveLock
1: begin;
1: explain select * from t_lockmods_rep for update;
1: select * from t_lockmods_rep for update;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.5 test order-by's plan
1: begin;
1: explain select * from t_lockmods order by c for update;
1: select * from t_lockmods order by c for update;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.6 select for update NOWAIT/SKIP LOCKED
-- NOWAIT/SKIP LOCKED should not affect the table-level lock
1: begin;
1: select * from t_lockmods for share;
2&: select * from t_lockmods for update nowait;
1: abort;
2<:

1: begin;
1: select * from t_lockmods for share;
2&: select * from t_lockmods for update skip locked;
1: abort;
2<:

1q:
2q:

-- 1.8 Test on DML lock behavior on Partition tables on QDs.
-- This suite will test:
--   * DML on root
--   * DML on one specific leaf
-- For detailed behavior and notes, please refer below
-- cases's comments.
-- Details: https://groups.google.com/a/greenplum.org/g/gpdb-dev/c/wAPKpJzhbpM
-- start_ignore
1:DROP TABLE IF EXISTS t_lockmods_part_tbl_upd_del;
-- end_ignore

1:CREATE TABLE t_lockmods_part_tbl_upd_del (a int, b int, c int) PARTITION BY RANGE(b) (START(1) END(3) EVERY(1));
1:INSERT INTO t_lockmods_part_tbl_upd_del SELECT i, 1, i FROM generate_series(1,10)i;

-- 
1: BEGIN;
1: DELETE FROM t_lockmods_part_tbl_upd_del;
-- on QD, there's a lock on the root and the target partition
1: select * from show_locks_lockmodes;
1: ROLLBACK;

--
-- The session cannot be reused.
--
-- The macro RELCACHE_FORCE_RELEASE is defined iff USE_ASSERT_CHECKING is
-- defined, and when RELCACHE_FORCE_RELEASE is defined the relcache is
-- forcefully released when closing the relation.
--
-- The function generate_partition_qual() will behave differently depends on
-- the existence of the relcache.
--
-- - if the relation is not cached, it will open it in AccessShareLock mode,
--   and save the relpartbound in the relcache;
-- - if the relation is already cached, it will load the relpartbound from the
--   cache directly without opening the relation;
--
-- So as a result, in the following transactions we will see an extra
-- AccessShareLock lock in a --enable-cassert build compared to a
-- --disable-cassert build.
--
-- To make the test results stable, we do not reuse the sessions in the test,
-- all the tests are performed without the relcache.
1q:

1: BEGIN;
1: UPDATE t_lockmods_part_tbl_upd_del SET c = 1 WHERE c = 1;
-- on QD, there's a lock on the root and the target partition
1: select * from show_locks_lockmodes;
1: ROLLBACK;
1q:

1: BEGIN;
1: DELETE FROM t_lockmods_part_tbl_upd_del_1_prt_1;
-- since the delete operation is on leaf part, there will be a lock on QD
1: select * from show_locks_lockmodes;
1: ROLLBACK;
1q:

1: BEGIN;
1: UPDATE t_lockmods_part_tbl_upd_del_1_prt_1 SET c = 1 WHERE c = 1;
-- since the update operation is on leaf part, there will be a lock on QD
1: select * from show_locks_lockmodes;
1: ROLLBACK;
1q:

TRUNCATE t_lockmods_ao;
