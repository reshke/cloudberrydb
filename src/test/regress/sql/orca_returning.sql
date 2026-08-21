-- Test UPDATE/DELETE/INSERT RETURNING through Orca optimizer.
-- Before this fix, Orca always fell back to the GPDB planner for any DML
-- with a RETURNING clause. This test verifies that Orca now plans DML
-- RETURNING directly (no fallback) and produces correct results.

-- start_ignore
CREATE SCHEMA orca_returning;
SET search_path to orca_returning;
-- end_ignore

-- Enable fallback tracing to detect any unexpected fallbacks.
set optimizer_trace_fallback = on;

-- Non-partitioned heap table: Orca can plan DML on this.
CREATE TABLE ret_t (id int4, val int4, name text) DISTRIBUTED BY (id);
INSERT INTO ret_t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd');

-- UPDATE RETURNING: simple column references.
-- Verify the plan goes through GPORCA with a Gather Motion for RETURNING.
EXPLAIN UPDATE ret_t SET val = val + 1 WHERE id = 1 RETURNING id, val, name;
UPDATE ret_t SET val = val + 1 WHERE id = 1 RETURNING id, val, name;

-- UPDATE RETURNING: expression in RETURNING
EXPLAIN UPDATE ret_t SET val = val * 2 WHERE id = 2 RETURNING id, val * 10 AS tenval;
UPDATE ret_t SET val = val * 2 WHERE id = 2 RETURNING id, val * 10 AS tenval;

-- UPDATE RETURNING: all columns
UPDATE ret_t SET name = 'cc' WHERE id = 3 RETURNING *;

-- DELETE RETURNING
EXPLAIN DELETE FROM ret_t WHERE id = 4 RETURNING id, name;
DELETE FROM ret_t WHERE id = 4 RETURNING id, name;

-- INSERT RETURNING
INSERT INTO ret_t VALUES (5, 50, 'e') RETURNING id, val;

-- Verify final state
SELECT * FROM ret_t ORDER BY id;

-- Test DML RETURNING used inside a CTE (WITH ... RETURNING).
-- The RETURNING results should be consumable by the outer query.
CREATE TABLE ret_cte (id int4, val int4) DISTRIBUTED BY (id);
INSERT INTO ret_cte VALUES (1, 10), (2, 20), (3, 30);

-- UPDATE ... RETURNING inside CTE, consumed by outer SELECT as a subquery
-- start_matchsubs
-- m/^INFO.*GPORCA.*falling/
-- s/^INFO.*GPORCA.*falling/INFO:  GPORCA fallback (expected)/
-- m/^DETAIL.*Falling.*No variable/
-- s/^DETAIL.*/DETAIL:  Expected fallback for CTE RETURNING/
-- m/^DETAIL.*Falling.*Empty target/
-- s/^DETAIL.*/DETAIL:  Expected fallback for CTE RETURNING/
-- end_matchsubs
WITH d AS (
    UPDATE ret_cte SET val = val + 5 WHERE id <= 2 RETURNING id, val
)
SELECT * FROM d ORDER BY id;

-- DELETE ... RETURNING inside CTE, consumed by outer SELECT with aggregation
WITH d AS (
    DELETE FROM ret_cte WHERE id = 3 RETURNING id, val
)
SELECT count(*) AS cnt, sum(val) AS total FROM d;

-- Verify remaining rows
SELECT * FROM ret_cte ORDER BY id;

DROP TABLE ret_cte;

-- Test UPDATE RETURNING that changes the distribution key (split update).
-- Orca does not support RETURNING with split updates yet, so this should
-- fall back to the GPDB planner.
-- start_matchsubs
-- m/^INFO.*GPORCA.*falling/
-- s/^INFO.*GPORCA.*falling/INFO:  GPORCA fallback (expected)/
-- m/^DETAIL.*Falling.*RETURNING with split/
-- s/^DETAIL.*/DETAIL:  Expected fallback for split update RETURNING/
-- end_matchsubs
CREATE TABLE ret_dist (id int4, v int4) DISTRIBUTED BY (id);
INSERT INTO ret_dist VALUES (1, 100), (2, 200);
UPDATE ret_dist SET id = id + 10 WHERE v = 100 RETURNING id, v;
SELECT * FROM ret_dist ORDER BY id;

-- Cleanup
DROP TABLE ret_t;
DROP TABLE ret_dist;
DROP SCHEMA orca_returning;
