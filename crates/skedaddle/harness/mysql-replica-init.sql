-- The entrypoint initializes accounts first; the final server boots read-only.
-- The runner briefly disables these only to load the consistent source snapshot.
SET PERSIST_ONLY read_only=ON;
SET PERSIST_ONLY super_read_only=ON;
