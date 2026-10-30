Reproducer for the review findings on PR #2204 (feat/ao-no-rewrite-port,
head 6d543f211). Differential H/B setup: base = b6104f71 (this branch's
parent), head = PR head. Run against both clusters; compare outputs.

Findings (head-only, 3/3 deterministic):

1) Ghost rows: in-place narrowed-column ALTER COLUMN TYPE on an AOCS
   table with >=3 INSERT groups (segment groups), duplicate keys in a
   later group, and a DELETE touching those duplicates makes the
   narrowed column's scans return DELETED rows:

    SELECT count(*), count(c7) FROM repro.t;
    base: 382 / 382     head: 382 / 385   <- 3 ghost rows

   Ghosts are exactly the values of the deleted duplicate-key copies
   (k % 19 = 0 re-inserted by generate_series(1,97) after (1..150,
   151..300)). count(*) and non-rewritten columns stay consistent, so
   any single-column read of the narrowed column returns 385 rows vs
   382 by key: internal result deviation.

2) ANALYZE crash: ANALYZE on such a head table SIGABRTs the QE backend
   in gp_acquire_sample_rows (signal 6, no TRAP text). Reproduces on
   ANY head AOCS table with >=3 insert groups after a narrowed ALTER,
   including tables where visible row counts remain consistent (latent
   structural inconsistency of the narrowed column file); 2-group
   tables and AO-row tables are unaffected. 9/9 + 5/5 in targeted runs,
   17 occurrences in the 2-hour differential soak.

Unified verdict: aocs_writecol_rewritesegfiles (the column-only rewrite
from commit 6d543f211, scanning under SnapshotAny to keep row numbers
aligned) produces a structurally inconsistent narrowed column file when
the table has >=3 segment groups; deleted-row visibility via
AppendOnlyVisimap is lost, and the per-column physical read path trips
over the inconsistency during ANALYZE sampling.

Ablations (all on identical differential clusters):
- 2 insert groups: clean at any size (up to 1882 rows); 3 groups
  diverge at 382 rows.
- duplicate keys, DELETE and the narrowing ALTER all required;
  compression (zstd), blocksize, WHERE filters, NULLs, SET/DROP NOT
  NULL, extra columns: irrelevant.
- AO row orientation: clean.
