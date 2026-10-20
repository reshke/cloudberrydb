-- start_ignore
DROP SCHEMA IF EXISTS anp_test CASCADE;
CREATE SCHEMA anp_test;
SET search_path TO anp_test;
-- end_ignore

-- ANALYZE ROOTPARTITION on non-partitioned relations used to emit the
-- misleading "cannot analyze a non-root partition" message (A13).  It must
-- name the real problem for non-partitioned relkinds, keep the dedicated
-- message for non-root partitions, and leave regular ANALYZE working.

CREATE TABLE d_tab (a int, b int) DISTRIBUTED BY (a);
INSERT INTO d_tab SELECT i, i FROM generate_series(1, 5) i;

CREATE VIEW v_t AS SELECT 1;

CREATE SEQUENCE s_q;

CREATE INDEX i_x ON d_tab (a);

CREATE TABLE d_root (a int, b int) PARTITION BY RANGE (b) DISTRIBUTED BY (a);
CREATE TABLE d_p1 PARTITION OF d_root FOR VALUES FROM (1) TO (10);
CREATE TABLE d_p2 PARTITION OF d_root FOR VALUES FROM (10) TO (20);
INSERT INTO d_root SELECT i, i FROM generate_series(1, 19) i;

-- non-partitioned relations are skipped, naming the real problem
ANALYZE ROOTPARTITION d_tab;
ANALYZE ROOTPARTITION v_t;
ANALYZE ROOTPARTITION s_q;
ANALYZE ROOTPARTITION i_x;

-- non-root partitions keep their dedicated message
ANALYZE ROOTPARTITION d_p1;

-- positive cases: these must actually analyze, with no warnings
ANALYZE ROOTPARTITION d_root;
ANALYZE d_root;
ANALYZE d_p1;
ANALYZE d_tab;

-- start_ignore
DROP SCHEMA anp_test CASCADE;
-- end_ignore
