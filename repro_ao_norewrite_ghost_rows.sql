-- Minimal repro for apache/cloudberry PR #2204 (feat/ao-no-rewrite-port, head 6d543f211)
-- In-place narrowed-column ALTER on AOCS table exposes DELETED rows in
-- column scans of the narrowed column (count(c7) > count(*), ghost values).
-- Requires >= 3 INSERT groups (appendonly segment groups) with duplicate
-- keys in a later group, a DELETE touching those duplicates, then the
-- narrowing ALTER. Base (b6104f71) is consistent; PR head diverges.

CREATE SCHEMA IF NOT EXISTS repro;
DROP TABLE IF EXISTS repro.t;

CREATE TABLE repro.t (k int, c7 varchar(30))
  WITH (appendonly=true, orientation=column)
  DISTRIBUTED BY (k);

INSERT INTO repro.t SELECT g, 'x' FROM generate_series(1, 150) g;
INSERT INTO repro.t SELECT g, 'x' FROM generate_series(151, 300) g;
INSERT INTO repro.t SELECT g, 'x' FROM generate_series(1, 97) g;  -- duplicate keys

DELETE FROM repro.t WHERE k % 19 = 0 AND k < 200;

ALTER TABLE repro.t ALTER COLUMN c7 TYPE varchar(8);

-- base (main b6104f71):        count=382  count(c7)=382   (tuple rewrite)
-- head (PR #2204 6d543f211):   count=382  count(c7)=385   <-- 3 ghost rows
SELECT count(*), count(c7) FROM repro.t;

-- ghosts are values of DELETED rows that had a duplicate copy in a later group:
SELECT c7, count(*) FROM repro.t WHERE k % 19 = 0 AND k < 200 GROUP BY 1;
