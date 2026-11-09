-- case: copy_to_csv
-- title: COPY TO STDOUT in CSV format
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_copy_to_csv (id int PRIMARY KEY);
COPY r0c_copy_to_csv TO STDOUT WITH (FORMAT csv);
