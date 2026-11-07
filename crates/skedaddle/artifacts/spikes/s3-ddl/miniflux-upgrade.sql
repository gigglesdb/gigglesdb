SET search_path=miniflux,public; BEGIN;
create index entries_user_status_idx on entries(user_id, status);
create index feeds_user_category_idx on feeds(user_id, category_id);

DELETE FROM schema_version; INSERT INTO schema_version VALUES ('13'); COMMIT; SET search_path=public;
