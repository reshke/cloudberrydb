-- Test to make sure FTS doesn't mark primary down if its recovering. Fault
-- 'fts_conn_startup_packet' is used to simulate the primary responding
-- in-recovery to FTS, primary is not actually going through crash-recovery in
-- test.
-- Probe before injecting recovery faults: a mirror may still be catching up
-- after an earlier test, and FTS can only promote a synchronized mirror.
create function pg_temp.wait_for_fts_sync(timeout_seconds integer default 120)
returns void as $$
declare
  deadline timestamptz := clock_timestamp() + timeout_seconds * interval '1 second';
begin
  loop
    perform gp_request_fts_probe_scan();
    if (select count(*) = 2 and bool_and(mode = 's' and status = 'u')
        from gp_segment_configuration where content = 0) then
      return;
    end if;
    if clock_timestamp() >= deadline then
      raise exception 'timed out waiting for content 0 primary and mirror to synchronize';
    end if;
    perform pg_sleep(0.1);
  end loop;
end;
$$ language plpgsql;
select pg_temp.wait_for_fts_sync();
select role, preferred_role, mode, status from gp_segment_configuration where content = 0 order by role;
select gp_inject_fault_infinite('fts_conn_startup_packet', 'skip', dbid)
from gp_segment_configuration where content = 0 and role = 'p';
-- to make test deterministic and fast
-- start_ignore
\!gpconfig -c gp_fts_probe_retries -v 2 --masteronly
-- end_ignore

-- Allow extra time for mirror promotion to complete recovery to avoid
-- gprecoverseg BEGIN failures due to gang creation failure as some primaries
-- are not up. Setting these increase the number of retries in gang creation in
-- case segment is in recovery. Approximately we want to wait 2 minutes at most.
-- start_ignore
\!gpconfig -c gp_gang_creation_retry_count -v 127 --skipvalidation --masteronly
\!gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --masteronly
\!gpstop -u
-- end_ignore
-- Wait a few seconds, to ensure the config changes take effect.
select pg_sleep(5);
select gp_request_fts_probe_scan();
-- start_ignore
\!gpfts -A -D
-- end_ignore
select gp_wait_until_triggered_fault('fts_conn_startup_packet', 3, dbid)
from gp_segment_configuration where content = 0 and role = 'p';
select role, preferred_role, mode, status from gp_segment_configuration where content = 0 order by role;

-- test other scenario where recovery on primary is hung and hence FTS marks
-- primary down and promotes mirror. When 'fts_recovery_in_progress' is set to
-- skip it mimics the behavior of hung recovery on primary.
select gp_inject_fault_infinite('fts_recovery_in_progress', 'skip', dbid)
from gp_segment_configuration where content = 0 and role = 'p';
-- We call gp_request_fts_probe_scan twice to guarantee that the scan happens
-- after the fts_recovery_in_progress fault has been injected. If periodic fts
-- probe is running when the first request scan is run it is possible to not
-- see the effect due to the fault.
select gp_request_fts_probe_scan();
select gp_request_fts_probe_scan();
-- start_ignore
\!gpfts -A -D
\!gpfts -A -D
-- end_ignore
-- Wait for the role change as well as the promoted mirror accepting queries.
do $$
declare
  deadline timestamptz := clock_timestamp() + interval '120 seconds';
begin
  loop
    perform gp_request_fts_probe_scan();
    if exists (select 1 from gp_segment_configuration
               where content = 0 and role = 'p' and preferred_role = 'm'
                 and status = 'u') then
      begin
        perform count(*) from gp_dist_random('gp_id');
        return;
      exception
        when others then
          -- Promotion can still be finishing after FTS changes the catalog.
          null;
      end;
    end if;
    if clock_timestamp() >= deadline then
      raise exception 'timed out waiting for content 0 mirror promotion';
    end if;
    perform pg_sleep(0.1);
  end loop;
end;
$$;

select role, preferred_role, mode, status from gp_segment_configuration where content = 0 order by role;

-- The remaining steps are to bring back the cluster to original state.
-- start_ignore

\! gprecoverseg -av --no-progress
\!gpfts -A -D
-- end_ignore

-- Wait for both catalog rows to report a live, synchronized segment.
select pg_temp.wait_for_fts_sync();
select role, preferred_role, mode, status from gp_segment_configuration where content = 0 order by role;

-- start_ignore
\! gprecoverseg -arv
\!gpfts -A -D
-- end_ignore

-- Wait for both catalog rows to report a live, synchronized segment.
select pg_temp.wait_for_fts_sync();
select role, preferred_role, mode, status from gp_segment_configuration where content = 0 order by role;

-- start_ignore
\!gpconfig -r gp_fts_probe_retries --masteronly
\!gpconfig -r gp_gang_creation_retry_count --skipvalidation --masteronly
\!gpconfig -r gp_gang_creation_retry_timer --skipvalidation --masteronly
\!gpstop -u
-- end_ignore

-- cleanup steps
select gp_inject_fault('all', 'reset', dbid)
from gp_segment_configuration where content = 0 and role = 'p';
