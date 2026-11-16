-- id: pgt_startup_datestyle
-- title: Startup pgt_startup_datestyle
-- owner: D
-- env: PGDATESTYLE=SQL, DMY
SELECT current_setting('DateStyle'), DATE '2024-01-02';
