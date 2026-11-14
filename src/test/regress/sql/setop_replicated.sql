-- A replicated data-modifying CTE combined with an Entry-locus setop
-- must be reduced to a singleton input before the set operation.
SET optimizer = off;
CREATE TABLE setop_replicated (v int) DISTRIBUTED REPLICATED;
WITH ins AS (INSERT INTO setop_replicated VALUES (1), (1) RETURNING v)
SELECT v FROM ins
INTERSECT ALL
SELECT 1::int FROM gp_segment_configuration WHERE content = -1;
WITH ins AS (INSERT INTO setop_replicated VALUES (2), (2) RETURNING v)
SELECT v FROM ins
EXCEPT ALL
SELECT 2::int FROM gp_segment_configuration WHERE content = -1;
\echo Reverse operand order exercises Entry on the left of the set operation.
WITH ins AS (INSERT INTO setop_replicated VALUES (3), (3) RETURNING v)
SELECT 3::int FROM gp_segment_configuration WHERE content = -1
INTERSECT ALL
SELECT v FROM ins;
WITH ins AS (INSERT INTO setop_replicated VALUES (4), (4) RETURNING v)
SELECT 4::int FROM gp_segment_configuration WHERE content = -1
EXCEPT ALL
SELECT v FROM ins;
DROP TABLE setop_replicated;
RESET optimizer;
