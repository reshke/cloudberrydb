-- MERGE on a replicated table.
--
-- The target is a replicated table; the source is hash-distributed. The
-- source must be broadcast to all segments, so that every segment performs
-- the same MERGE actions, keeping the replicas of the target identical.
-- (bug: upd_del_replicated_table flag was not set for CMD_MERGE, so each
-- segment only saw the source rows hashed to it -> per-seg divergence.)

CREATE TABLE mrg_repl_target (k int, v int) DISTRIBUTED REPLICATED;
CREATE TABLE mrg_repl_source (k int, v int) DISTRIBUTED BY (k);

INSERT INTO mrg_repl_source SELECT g, g * 10 FROM generate_series(1, 60) g;
-- keys 1..30 match, 31..60 do not
INSERT INTO mrg_repl_target SELECT g, 0 FROM generate_series(1, 30) g;

MERGE INTO mrg_repl_target t
USING mrg_repl_source s
ON t.k = s.k
WHEN MATCHED THEN
	UPDATE SET v = s.v
WHEN NOT MATCHED THEN
	INSERT VALUES (s.k, s.v);

-- logical result, as if a single-node MERGE had executed
SELECT count(*), sum(v) FROM mrg_repl_target;

-- gp_dist_random returns one row copy per segment, so the total is
-- numsegments * logical_count when all replicas are identical
-- (pre-fix: sum of diverging per-seg counts, less than that)
SELECT count(*) FROM gp_dist_random('mrg_repl_target');

-- the inserted rows must exist on every segment (pre-fix, they were
-- scattered: each segment inserted only its own subset of source rows)
SELECT count(*) FROM gp_dist_random('mrg_repl_target') WHERE k > 30;
SELECT count(*) FROM gp_dist_random('mrg_repl_target') WHERE k <= 30 AND v = 0;
SELECT count(*) FROM mrg_repl_target WHERE v = 0;

-- SPI: ROW_COUNT after a MERGE on a replicated table must be the logical
-- number of merged rows, not a per-segment sum/subset
CREATE OR REPLACE FUNCTION mrg_repl_rowcount() RETURNS bigint AS $$
DECLARE rc bigint;
BEGIN
	EXECUTE 'MERGE INTO mrg_repl_target t USING mrg_repl_source s ON t.k = s.k ' ||
	        'WHEN MATCHED THEN UPDATE SET v = s.v + 1';
	GET DIAGNOSTICS rc = ROW_COUNT;
	RETURN rc;
END $$ LANGUAGE plpgsql;

TRUNCATE mrg_repl_target;
INSERT INTO mrg_repl_target SELECT g, 0 FROM generate_series(1, 30) g;

SELECT mrg_repl_rowcount();
SELECT count(*), sum(v) FROM mrg_repl_target;

DROP FUNCTION mrg_repl_rowcount();
DROP TABLE mrg_repl_target;
DROP TABLE mrg_repl_source;
