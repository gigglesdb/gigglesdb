-- case: copy_to_csv
-- title: COPY TO forms other than text STDOUT
-- owner: E2
-- expect: intentional COPY TO forms other than text STDOUT
CREATE TABLE r0c_copy_to_csv (id int PRIMARY KEY);
-- expect-sqlstate: 0A000
COPY r0c_copy_to_csv TO STDOUT WITH (FORMAT csv);
