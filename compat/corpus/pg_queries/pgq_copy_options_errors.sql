-- case: pgq_copy_options_errors
-- title: COPY option validation
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_errors (id int, a text, b text);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (HEADER 2);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (HEADER MATCH);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (FORMAT binary, HEADER);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (FORMAT xml);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (bogus 1);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (FORMAT csv, QUOTE '~~');
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (HEADER, HEADER false);
-- @error
COPY t_pgq_copy_options_errors TO STDOUT WITH (FORMAT csv, FORCE_NULL (a));
