-- inh_mix_dist: inheritance trees must not mix distributed and
-- replicated/entry-only tables, even with explicit DISTRIBUTED RANDOMLY
-- on the child. Previously the explicit-random path in
-- transformDistributedBy() skipped parent distribution checks, letting
-- a randomly-distributed child attach to a replicated parent and
-- poisoning the whole inheritance tree.
CREATE SCHEMA inh_mix_dist_schema;
SET search_path = inh_mix_dist_schema;

CREATE TABLE hash_parent (a int, b text) DISTRIBUTED BY (a);

-- A replicated parent cannot get any distributed child, hash or random
CREATE TABLE repl_parent (a int, b text) DISTRIBUTED REPLICATED;
INSERT INTO repl_parent VALUES (1, 'p1'), (2, 'p2');

-- hash child: was already rejected
CREATE TABLE repl_child_hash (a int, b text) INHERITS (repl_parent) DISTRIBUTED BY (a);

-- random child: must be rejected the same way
CREATE TABLE repl_child_random (a int, b text) INHERITS (repl_parent) DISTRIBUTED RANDOMLY;

-- replicated child: was already rejected
CREATE TABLE repl_child_replicated (a int, b text) INHERITS (repl_parent) DISTRIBUTED REPLICATED;

-- no explicit distribution: was already rejected
CREATE TABLE repl_child_nodefault (a int, b text) INHERITS (repl_parent);

-- replicated parent must be unaffected (note: tableoid on replicated tables
-- is a separate, known limitation and not used here)
SELECT a, b FROM repl_parent ORDER BY a;
UPDATE repl_parent SET b = b || '_u' WHERE a = 1;
SELECT a, b FROM repl_parent ORDER BY a;
DELETE FROM repl_parent WHERE a = 2;
SELECT a, b FROM repl_parent ORDER BY a;

-- legal hash inheritance tree keeps working
CREATE TABLE hash_child (a int, b text) INHERITS (hash_parent) DISTRIBUTED BY (a);
INSERT INTO hash_parent VALUES (10, 'hp');
INSERT INTO hash_child VALUES (11, 'hc');
SELECT tableoid::regclass AS rel, a, b FROM hash_parent ORDER BY a;
UPDATE hash_parent SET b = b || '_u' WHERE a = 10;
SELECT a, b FROM hash_parent ORDER BY a;
DELETE FROM hash_parent WHERE a = 11;
SELECT count(*) FROM hash_parent;

-- ALTER TABLE ... INHERIT guard is unchanged
CREATE TABLE orphan_hash (a int, b text) DISTRIBUTED BY (a);
ALTER TABLE orphan_hash INHERIT repl_parent;

-- cleanup
RESET search_path;
DROP SCHEMA inh_mix_dist_schema CASCADE;
