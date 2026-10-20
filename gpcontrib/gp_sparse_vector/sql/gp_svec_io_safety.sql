CREATE EXTENSION gp_sparse_vector;
SET search_path TO sparse_vector;

-- IO safety regression tests for svec input function.
--
-- Malformed run-length lists used to be accepted by svec_in and produced
-- internally corrupt (or colliding) RLE indices and wrong dimensions:
--   * zero/negative run lengths corrupt the RLE index entries
--   * a run-length sum exceeding INT32_MAX silently wraps the dimension,
--     which later drives out-of-bounds writes in sdata_to_float8arr.
-- All of these must now be rejected at input time.

-- Valid vectors must still work and round-trip through svec_out.
SELECT '{1}:{1.5}'::svec;
SELECT '{1,2}:{1.5,2.5}'::svec;
SELECT '{}'::svec;
SELECT '{2147483647}:{1}'::svec;
SELECT svec_return_array('{1,2}:{1.5,2.5}'::svec);

-- Negative run lengths: rejected.
SELECT '{-1}:{2}'::svec;
SELECT '{-1,-1}:{1,1}'::svec;
SELECT '{1,-200}:{1,1}'::svec;
SELECT '{-200}:{1}'::svec;

-- Zero run lengths: rejected.
SELECT '{0,0}:{1,1}'::svec;
SELECT '{0}:{1}'::svec;
SELECT '{1,0}:{1,1}'::svec;

-- Run-length sums above INT32_MAX: rejected instead of wrapping the
-- dimension (this exact input used to crash the backend with SIGSEGV in
-- sdata_to_float8arr when cast to float8[]).
SELECT '{2147483648}:{1}'::svec;
SELECT '{2147483648,2147483648,5}:{1,2,3}'::svec;
SELECT '{2147483647,1}:{1,2}'::svec;

-- Mismatched counts are still rejected.
SELECT '{1,2}:{1}'::svec;
