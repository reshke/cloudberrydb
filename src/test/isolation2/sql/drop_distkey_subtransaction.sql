-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE drop_distkey_ao (a int, b int) WITH (appendonly=true) DISTRIBUTED BY (a);
CREATE TABLE drop_distkey_heap (a int, b int) DISTRIBUTED BY (a);

-- Give both tables a new relfilenumber in the parent transaction.  This
-- keeps their relcache entries alive across the subtransaction abort even
-- in cassert builds, where RELCACHE_FORCE_RELEASE would otherwise mask
-- an incorrectly modified cached policy.
1: BEGIN;
1: TRUNCATE drop_distkey_ao, drop_distkey_heap;
1: INSERT INTO drop_distkey_ao SELECT i, i FROM generate_series(1, 10) i;
1: INSERT INTO drop_distkey_heap SELECT i, i FROM generate_series(1, 10) i;

-- Load both relcache entries before the catalog is locked, so the ALTERs
-- time out while replacing the policy rather than while loading it.
1: SELECT count(*) FROM drop_distkey_ao;
1: SELECT count(*) FROM drop_distkey_heap;
1: SAVEPOINT s1;
2: BEGIN;
2: LOCK TABLE gp_distribution_policy IN ACCESS EXCLUSIVE MODE;
1: SET LOCAL lock_timeout = '1s';
1: ALTER TABLE drop_distkey_ao DROP COLUMN a;
1: ROLLBACK TO s1;
1: SAVEPOINT s2;
1: SET LOCAL lock_timeout = '1s';
1: ALTER TABLE drop_distkey_heap DROP COLUMN a;
1: ROLLBACK TO s2;
2: COMMIT;

-- The catalog and relcache must still describe the original hash keys.
1: SELECT localoid::regclass AS table_name, distkey FROM gp_distribution_policy WHERE localoid IN ('drop_distkey_ao'::regclass, 'drop_distkey_heap'::regclass) ORDER BY localoid::regclass::text;
1: ALTER TABLE drop_distkey_ao DROP COLUMN a;
1: ALTER TABLE drop_distkey_heap DROP COLUMN a;
1: COMMIT;

-- Retrying the DROP must switch both tables to random distribution.
1: SELECT localoid::regclass AS table_name, distkey FROM gp_distribution_policy WHERE localoid IN ('drop_distkey_ao'::regclass, 'drop_distkey_heap'::regclass) ORDER BY localoid::regclass::text;
1: SELECT bool_and(distkey = ''::int2vector) AS segments_random FROM gp_dist_random('gp_distribution_policy') WHERE localoid IN ('drop_distkey_ao'::regclass, 'drop_distkey_heap'::regclass);
-- Check reads and writes in a fresh session as well.
3: INSERT INTO drop_distkey_ao VALUES (42);
3: INSERT INTO drop_distkey_heap VALUES (42);
3: SELECT count(*), sum(b) FROM drop_distkey_ao;
3: SELECT count(*), sum(b) FROM drop_distkey_heap;
DROP TABLE drop_distkey_ao, drop_distkey_heap;

-- Also roll back a successful DROP, after its command-counter increment has
-- already made the random policy visible, then retry in the same session.
CREATE TABLE drop_distkey_rollback (a int, b int, c int) DISTRIBUTED BY (a, b);
INSERT INTO drop_distkey_rollback VALUES (1, 2, 3);
1: BEGIN;
1: SAVEPOINT s;
1: ALTER TABLE drop_distkey_rollback DROP COLUMN a;
1: SELECT distkey FROM gp_distribution_policy WHERE localoid = 'drop_distkey_rollback'::regclass;
1: ROLLBACK TO s;
1: SELECT distkey FROM gp_distribution_policy WHERE localoid = 'drop_distkey_rollback'::regclass;
1: SELECT * FROM drop_distkey_rollback;
1: ALTER TABLE drop_distkey_rollback DROP COLUMN a, DROP COLUMN b;
1: COMMIT;
1: SELECT distkey FROM gp_distribution_policy WHERE localoid = 'drop_distkey_rollback'::regclass;
1: INSERT INTO drop_distkey_rollback VALUES (4);
1: SELECT c FROM drop_distkey_rollback ORDER BY c;
DROP TABLE drop_distkey_rollback;
