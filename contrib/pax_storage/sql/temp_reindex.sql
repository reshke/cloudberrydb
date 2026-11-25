-- REINDEX DATABASE must handle the auxiliary index of a temporary PAX table.
CREATE DATABASE pax_reindex_test_db TEMPLATE template0;
\c pax_reindex_test_db
CREATE TEMP TABLE pax_reindex_temp (a int) USING pax
  ON COMMIT DELETE ROWS DISTRIBUTED BY (a);
REINDEX DATABASE CONCURRENTLY pax_reindex_test_db;
BEGIN;
INSERT INTO pax_reindex_temp VALUES (1);
SELECT * FROM pax_reindex_temp;
COMMIT;
BEGIN;
INSERT INTO pax_reindex_temp VALUES (2);
SELECT * FROM pax_reindex_temp;
COMMIT;
DROP TABLE pax_reindex_temp;
\c pax_test
DROP DATABASE pax_reindex_test_db;
