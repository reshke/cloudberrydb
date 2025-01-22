-- Check changing table access method

-- Scenario 1: Heap to Heap
CREATE TABLE heap2heap(a int, b int) DISTRIBUTED BY (a);
CREATE TABLE heap2heap2(a int, b int) DISTRIBUTED BY (a);

INSERT INTO heap2heap SELECT i,i FROM generate_series(1,5) i;
INSERT INTO heap2heap2 SELECT i,i FROM generate_series(1,5) i;

CREATE TEMP TABLE relfilebeforeheap AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heap2heap', 'heap2heap2')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heap2heap', 'heap2heap2') ORDER BY segid;

-- changing to the same access method shouldn't rewrite the table
-- (i.e. the relfilenodes shouldn't change)
ALTER TABLE heap2heap SET ACCESS METHOD heap;
ALTER TABLE heap2heap2 SET WITH (appendoptimized=false);

CREATE TEMP TABLE relfileafterheap AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heap2heap', 'heap2heap2')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heap2heap', 'heap2heap2') ORDER BY segid;

-- relfilenodes shouldn't change
SELECT count(*) FROM (SELECT * FROM relfilebeforeheap UNION SELECT * FROM relfileafterheap)a;


-- Scenario 2: Heap to AO
CREATE TABLE heap2ao(a int, b int) WITH (fillfactor=70) DISTRIBUTED BY (a);
CREATE TABLE heap2ao2(a int, b int) DISTRIBUTED BY (a);

CREATE INDEX heapi ON heap2ao(b);
ALTER TABLE heap2ao2 ADD CONSTRAINT unique_constraint UNIQUE (a);

INSERT INTO heap2ao SELECT i,i FROM generate_series(1,5) i;
INSERT INTO heap2ao2 SELECT i,i FROM generate_series(1,5) i;

-- Check reloptions once before altering.
SELECT reloptions from pg_class where relname in ('heap2ao', 'heap2ao2');

CREATE TEMP TABLE relfilebeforeao AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heap2ao', 'heap2ao2', 'heapi')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heap2ao', 'heap2ao2', 'heapi') ORDER BY segid;

-- Altering a heap table with a unique index to AO should error out
-- as unique indexes aren't supported on AO tables
ALTER TABLE heap2ao2 SET ACCESS METHOD ao_row;
ALTER TABLE heap2ao2 DROP CONSTRAINT unique_constraint;

-- Set default storage options for the table to inherit from
SET gp_default_storage_options = 'blocksize=65536, compresstype=zlib, compresslevel=5, checksum=true';

-- Alter table heap to AO should work
ALTER TABLE heap2ao SET ACCESS METHOD ao_row;
ALTER TABLE heap2ao2 SET WITH (appendoptimized=true);

-- The altered tables should have AO AM
SELECT c.relname, a.amname FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'heap2ao%';

-- The altered tables should inherit storage options from gp_default_storage_options
-- And, the original heap reloptions are gone (in this case, 'fillfactor'). 
SELECT blocksize,compresslevel,checksum,compresstype,columnstore
FROM pg_appendonly WHERE relid in ('heap2ao'::regclass::oid, 'heap2ao2'::regclass::oid);
SELECT reloptions from pg_class where relname in ('heap2ao', 'heap2ao2');

-- Check data is intact
SELECT * FROM heap2ao;
SELECT * FROM heap2ao2;

-- The tables and indexes should have been rewritten (should have different relfilenodes)
CREATE TEMP TABLE relfileafterao AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heap2ao', 'heap2ao2', 'heapi')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heap2ao', 'heap2ao2', 'heapi') ORDER BY segid;

SELECT * FROM relfilebeforeao INTERSECT SELECT * FROM relfileafterao;

-- aux tables are created, pg_appendonly row is created
-- FIXME: add check for gp_aoblkdir
SELECT * FROM gp_toolkit.__gp_aoseg('heap2ao');
SELECT * FROM gp_toolkit.__gp_aovisimap('heap2ao');
SELECT * FROM gp_toolkit.__gp_aoseg('heap2ao2');
SELECT * FROM gp_toolkit.__gp_aovisimap('heap2ao2');

-- pg_appendonly should have entries associated with the new AO tables
SELECT c.relname FROM pg_class c, pg_appendonly p WHERE c.relname LIKE 'heap2ao%' AND c.oid = p.relid;

-- check inherited tables
CREATE TABLE heapbase (a int, b int);
CREATE TABLE heapchild (c int) INHERITS (heapbase);
CREATE TABLE heapbase2 (a int, b int);
CREATE TABLE heapchild2 (c int) INHERITS (heapbase2);

INSERT INTO heapbase SELECT i,i FROM generate_series(1,5) i;
INSERT INTO heapchild SELECT i,i,i FROM generate_series(1,5) i;
INSERT INTO heapbase2 SELECT i,i FROM generate_series(1,5) i;
INSERT INTO heapchild2 SELECT i,i,i FROM generate_series(1,5) i;

CREATE TEMP TABLE inheritrelfilebefore AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heapbase', 'heapbase2')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heapbase', 'heapbase2') ORDER BY segid;

CREATE TEMP TABLE inheritchildrelfilebefore AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heapchild', 'heapchild2')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heapchild', 'heapchild2') ORDER BY segid;

ALTER TABLE heapbase SET ACCESS METHOD ao_row;
ALTER TABLE heapbase2 SET WITH (appendoptimized=true);

-- The altered tables should inherit storage options from gp_default_storage_options
show gp_default_storage_options;
SELECT blocksize,compresslevel,checksum,compresstype,columnstore
FROM pg_appendonly WHERE relid in ('heapbase'::regclass::oid, 'heapbase2'::regclass::oid);
SELECT reloptions from pg_class where relname in ('heapbase','heapbase2');

SELECT blocksize,compresslevel,checksum,compresstype,columnstore
FROM pg_appendonly WHERE relid in ('heapchild'::regclass::oid, 'heapchild2'::regclass::oid);
SELECT reloptions from pg_class where relname in ('heapchild','heapchild2');

-- The altered parent tables should have AO AM but child tables are still heap
SELECT c.relname, a.amname FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'heapbase%' OR c.relname LIKE 'heapchild%';

-- Check data is intact
SELECT * FROM heapbase;
SELECT * FROM heapbase2;

-- relfile node should change for base table set to AO
CREATE TEMP TABLE inheritrelfileafter AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heapbase', 'heapbase2')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heapbase', 'heapbase2') ORDER BY segid;

SELECT * FROM inheritrelfilebefore INTERSECT SELECT * FROM inheritrelfileafter;

-- relfile node should not change for child table
CREATE TEMP TABLE inheritchildrelfileafter AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('heapchild', 'heapchild2')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('heapchild', 'heapchild2') ORDER BY segid;

SELECT count(*) FROM (SELECT * FROM inheritchildrelfilebefore UNION SELECT * FROM inheritchildrelfileafter)a;

-- aux tables are created, pg_appendonly row is created
SELECT * FROM gp_toolkit.__gp_aoseg('heapbase');
SELECT * FROM gp_toolkit.__gp_aovisimap('heapbase');
SELECT * FROM gp_toolkit.__gp_aoseg('heapbase2');
SELECT * FROM gp_toolkit.__gp_aovisimap('heapbase2');
SELECT c.relname FROM pg_class c, pg_appendonly p WHERE c.relname LIKE 'heapbase%' AND c.oid = p.relid;

-- aux tables are not created for child table
SELECT * FROM gp_toolkit.__gp_aoseg('heapchild');
SELECT * FROM gp_toolkit.__gp_aovisimap('heapchild');
SELECT * FROM gp_toolkit.__gp_aoseg('heapchild2');
SELECT * FROM gp_toolkit.__gp_aovisimap('heapchild2');
SELECT c.relname FROM pg_class c, pg_appendonly p WHERE c.relname LIKE 'heapchild%' AND c.oid = p.relid;

-- Scenario 3: AO to Heap
SET gp_default_storage_options = 'blocksize=65536, compresstype=zlib, compresslevel=5, checksum=true';

CREATE TABLE ao2heap(a int, b int) WITH (appendonly=true);
CREATE TABLE ao2heap2(a int, b int) WITH (appendonly=true);
CREATE INDEX aoi ON ao2heap(b);

INSERT INTO ao2heap SELECT i,i FROM generate_series(1,5) i;
INSERT INTO ao2heap2 SELECT i,i FROM generate_series(1,5) i;

-- Check once that the AO tables have the custom reloptions 
SELECT relname, reloptions FROM pg_class WHERE relname LIKE 'ao2heap%';

-- Check once that the AO tables have relfrozenxid = 0
SELECT relname, relfrozenxid FROM pg_class WHERE relname LIKE 'ao2heap%';

CREATE TEMP TABLE relfilebeforeao2heap AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('ao2heap', 'ao2heap2', 'aoi')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('ao2heap', 'ao2heap2', 'aoi') ORDER BY segid;

-- Altering AO to heap
ALTER TABLE ao2heap SET ACCESS METHOD heap;
ALTER TABLE ao2heap2 SET WITH (appendoptimized=false);

-- The tables and indexes should have been rewritten (should have different relfilenodes)
CREATE TEMP TABLE relfileafterao2heap AS
    SELECT -1 segid, relfilenode FROM pg_class WHERE relname in ('ao2heap', 'ao2heap2', 'aoi')
    UNION SELECT gp_segment_id segid, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname in ('ao2heap', 'ao2heap2', 'aoi') ORDER BY segid;

SELECT * FROM relfilebeforeao2heap INTERSECT SELECT * FROM relfileafterao2heap;

-- Check data is intact
SELECT * FROM ao2heap;
SELECT * FROM ao2heap2;

-- No AO aux tables should be left
SELECT * FROM gp_toolkit.__gp_aoseg('ao2heap');
SELECT * FROM gp_toolkit.__gp_aovisimap('ao2heap');
SELECT * FROM gp_toolkit.__gp_aoseg('ao2heap2');
SELECT * FROM gp_toolkit.__gp_aovisimap('ao2heap2');

-- No pg_appendonly entries should be left too
SELECT c.relname FROM pg_class c, pg_appendonly p WHERE c.relname LIKE 'ao2heap%' AND c.oid = p.relid;

-- The altered tables should have heap AM.
SELECT c.relname, a.amname FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ao2heap%';

-- The new heap tables shouldn't have the old AO table's reloptions
SELECT relname, reloptions FROM pg_class WHERE relname LIKE 'ao2heap%';

-- The new heap tables should have a valid relfrozenxid
SELECT relname, relfrozenxid <> '0' FROM pg_class WHERE relname LIKE 'ao2heap%';

DROP TABLE ao2heap;
DROP TABLE ao2heap2;

-- Scenario 4: Set reloptions along with change of AM.

CREATE TABLE ataoset(a int);
CREATE TABLE ataoset2(a int);
INSERT INTO ataoset select * from generate_series(1, 5);
INSERT INTO ataoset2 select * from generate_series(1, 5);

-- Error: user specifies a different AM than the one indicated in the WITH clause
ALTER TABLE ataoset SET ACCESS METHOD ao_row WITH(appendonly=true, orientation=column);

-- Error: user specifiies AO reloption when altering an AO table to heap.
CREATE TABLE ao2heaperror (a int) WITH (appendonly=true);
ALTER TABLE ao2heaperror SET ACCESS METHOD heap WITH (blocksize=65536);
DROP TABLE ao2heaperror;

-- Scenario 4.1: change from heap to AO with customized storage options
CREATE TEMP TABLE relfilebeforeat AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

ALTER TABLE ataoset SET WITH (appendonly=true, blocksize=65536, compresslevel=7);
ALTER TABLE ataoset2 SET ACCESS METHOD ao_row WITH (blocksize=65536, compresslevel=7);

CREATE TEMP TABLE relfileafterat AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

-- relfilenode changed
SELECT * FROM relfilebeforeat INTERSECT SELECT * FROM relfileafterat;

DROP TABLE relfilebeforeat;
DROP TABLE relfileafterat;

-- table AMs are changed to AO, and reloptions changed to what we set
SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ataoset%';

-- data are intact
SELECT count(*) FROM ataoset;
SELECT count(*) FROM ataoset2;

-- Scenario 4.2. Alter the table w/ the exact same reloptions. 
-- AM, relfilenodes and reloptions all should remain the same
CREATE TEMP TABLE relfilebeforeat AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

-- Firstly alter them with the exact same options as case 1.
-- Secondly alter them with same option values but different order.
-- Neither should trigger table rewrite. 
ALTER TABLE ataoset SET WITH (appendonly=true, blocksize=65536, compresslevel=7);
ALTER TABLE ataoset SET WITH (appendonly=true, compresslevel=7, blocksize=65536);
ALTER TABLE ataoset2 SET ACCESS METHOD ao_row WITH (blocksize=65536, compresslevel=7);
ALTER TABLE ataoset2 SET ACCESS METHOD ao_row WITH (compresslevel=7, blocksize=65536);

CREATE TEMP TABLE relfileafterat AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

-- no change to relfilenode
SELECT * FROM relfilebeforeat EXCEPT SELECT * FROM relfileafterat;

DROP TABLE relfilebeforeat;
DROP TABLE relfileafterat;

-- table AMs are still AO, but reloptions should reflect the order of options in the most recent ALTER TABLE
SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ataoset%';

-- data still intact
SELECT count(*) FROM ataoset;
SELECT count(*) FROM ataoset2;

-- Scenario 4.3. Use the same syntax to alter from AO to AO, but specifying different storage options.
--         Table should be rewritten.
CREATE TEMP TABLE relfilebeforeao AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

ALTER TABLE ataoset SET WITH (appendonly=true, blocksize=32768);
ALTER TABLE ataoset2 SET ACCESS METHOD ao_row WITH (blocksize=32768);

CREATE TEMP TABLE relfileafterao AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

-- table is rewritten
SELECT * FROM relfilebeforeao INTERSECT SELECT * FROM relfileafterao;

-- reloptions changed too
SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ataoset%';

DROP TABLE relfilebeforeao;
DROP TABLE relfileafterao;

-- Scenario 4.4. Alter the tables back to heap and set some reloptions too.
CREATE TEMP TABLE relfilebeforeat AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

ALTER TABLE ataoset SET ACCESS METHOD heap WITH (fillfactor=70);
ALTER TABLE ataoset2 SET ACCESS METHOD heap WITH (fillfactor=70);

CREATE TEMP TABLE relfileafterat AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ataoset%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ataoset%' ORDER BY segid;

-- there's a table rewrite
SELECT * FROM relfilebeforeat INTERSECT SELECT * FROM relfileafterat;

DROP TABLE relfilebeforeat;
DROP TABLE relfileafterat;

-- reloptions and AM also changed
SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ataoset%';

DROP TABLE ataoset;
DROP TABLE ataoset2;

-- Scenario 5: AO to AOCO 
SET gp_default_storage_options = 'blocksize=65536, compresstype=zlib, compresslevel=5, checksum=true';
CREATE TABLE ao2co(a int, b int) WITH (appendonly=true);
CREATE TABLE ao2co2(a int, b int) WITH (appendonly=true);
CREATE TABLE ao2co3(a int, b int) WITH (appendonly=true);
CREATE TABLE ao2co4(a int, b int) WITH (appendonly=true);
CREATE INDEX index_ao2co ON ao2co(b);
CREATE INDEX index_ao2co3 ON ao2co3(b);

INSERT INTO ao2co SELECT i,i FROM generate_series(1,5) i;
INSERT INTO ao2co2 SELECT i,i FROM generate_series(1,5) i;
INSERT INTO ao2co3 SELECT i,i FROM generate_series(1,5) i;
INSERT INTO ao2co4 SELECT i,i FROM generate_series(1,5) i;

-- ERROR: conflicting storage option specified.
ALTER TABLE ao2co SET ACCESS METHOD ao_column WITH (appendoptimized=true, orientation=row);
-- Use of *both* ACCESS METHOD and WITH clauses is allowed, but we'll print a hint to indicate the redundancy.
ALTER TABLE ao2co SET ACCESS METHOD ao_row WITH (appendoptimized=true, orientation=row);

CREATE TEMP TABLE relfilebeforeao AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ao2co%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ao2co%' ORDER BY segid;

-- Check once the reloptions
SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ao2co%';

-- Altering AO to AOCO with various syntaxes, reloptions:
ALTER TABLE ao2co SET ACCESS METHOD ao_column;
ALTER TABLE ao2co2 SET WITH (appendoptimized=true, orientation=column);
ALTER TABLE ao2co3 SET ACCESS METHOD ao_column WITH (blocksize=32768, compresslevel=3);
ALTER TABLE ao2co4 SET WITH (appendoptimized=true, orientation=column, blocksize=32768, compresslevel=3);

-- The tables are rewritten
CREATE TEMP TABLE relfileafterao AS
    SELECT -1 segid, relname, relfilenode FROM pg_class WHERE relname LIKE 'ao2co%'
    UNION SELECT gp_segment_id segid, relname, relfilenode FROM gp_dist_random('pg_class')
    WHERE relname LIKE 'ao2co%' ORDER BY segid;

SELECT * FROM relfilebeforeao INTERSECT SELECT * FROM relfileafterao;
DROP TABLE relfilebeforeao;
DROP TABLE relfileafterao;

-- Check data is intact
SELECT count(*) FROM ao2co;
SELECT count(*) FROM ao2co2;
SELECT count(*) FROM ao2co3;
SELECT count(*) FROM ao2co4;

-- Aux tables should have been deleted for the old AO table and recreated for the new AOCO table
-- Only tested for 2 out of the 4 tables being created, where the tables were altered w/wo reloptions. 
-- No need to test the other ones created by the alternative syntax SET WITH().
SELECT * FROM gp_toolkit.__gp_aoseg('ao2co');
SELECT * FROM gp_toolkit.__gp_aovisimap('ao2co');
SELECT count(*) FROM gp_toolkit.__gp_aocsseg('ao2co');
SELECT * FROM gp_toolkit.__gp_aoblkdir('ao2co');
SELECT * FROM gp_toolkit.__gp_aoseg('ao2co3');
SELECT * FROM gp_toolkit.__gp_aovisimap('ao2co3');
SELECT count(*) FROM gp_toolkit.__gp_aocsseg('ao2co3');
SELECT * FROM gp_toolkit.__gp_aoblkdir('ao2co3');

-- pg_attribute_encoding should have columns for the AOCO table
SELECT c.relname, a.attnum, a.attoptions FROM pg_attribute_encoding a, pg_class c WHERE a.attrelid = c.oid AND c.relname LIKE 'ao2co%';

-- AM and reloptions changed accordingly
SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON c.relam = a.oid WHERE c.relname LIKE 'ao2co%';

-- pg_appendonly should reflect the changes in reloptions
SELECT c.relname,a.blocksize,a.compresslevel,a.checksum,a.compresstype,a.columnstore
FROM pg_appendonly a, pg_class c WHERE a.relid = c.oid AND relname like ('ao2co%');

DROP TABLE ao2co;
DROP TABLE ao2co2;
DROP TABLE ao2co3;
DROP TABLE ao2co4;

-- Final scenario: the iterations of altering table from storage type "A" to "B" and back to "A". 
-- The following cases will cover all variations of such iterations:
-- 1. Heap->AO->Heap->AO
-- (TODO) 2. AO->AOCO->AO->AOCO
-- (TODO) 3. Heap->AOCO->Heap->AOCO

-- 1. Heap->AO->Heap->AO
CREATE TABLE heapao(a int, b int);
CREATE INDEX heapaoindex ON heapao(b);
INSERT INTO heapao SELECT i,i FROM generate_series(1,5) i;

ALTER TABLE heapao SET ACCESS METHOD ao_row;
ALTER TABLE heapao SET ACCESS METHOD heap;
ALTER TABLE heapao SET ACCESS METHOD ao_row;

-- Just checking data is intact. 
SELECT count(*) FROM heapao;
DROP TABLE heapao;

