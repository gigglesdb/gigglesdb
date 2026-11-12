use super::DdlError;

pub(super) fn validate_id(id: &str) -> Result<(), DdlError> {
    if id.is_empty()
        || id.len() > 30
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(DdlError(
            "installation id must be 1..30 lowercase ASCII letters, digits or underscores".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerIntent {
    Install,
    Remove,
}

#[derive(Clone, Debug)]
pub struct TriggerSql {
    pub install: String,
    pub verify: String,
    pub remove: String,
}

impl TriggerSql {
    /// All referenced catalog fields/functions are present on PG16, 17 and 18.
    /// Reject future majors instead of silently assuming catalog compatibility.
    pub fn new(id: &str, server_version_num: u32) -> Result<Self, DdlError> {
        validate_id(id)?;
        let major = server_version_num / 10000;
        if !(16..=18).contains(&major) {
            return Err(DdlError(
                "DDL capture requires PostgreSQL 16, 17 or 18".into(),
            ));
        }
        let name = format!("skedaddle_ddl_{id}");
        // set_config(..., true) is SET LOCAL. The scalar subquery rejects
        // multiple installations; no installation skips setting an origin.
        // This also tags upgrade/removal transactions before dropping the hook.
        let administrative_origin = format!(
            "SELECT pg_catalog.set_config('skedaddle.admin_origin', installation, true) FROM (SELECT ({}) AS installation) ids WHERE installation IS NOT NULL;",
            super::ADMIN_INSTALLATION_SQL,
        );
        let body = format!(
            r#"
DECLARE details jsonb; v bigint;
BEGIN
  SELECT coalesce(jsonb_agg(jsonb_build_object(
    'classid',d.classid,'objid',d.objid,'objsubid',d.objsubid,
    'command_tag',d.command_tag,'object_type',d.object_type,
    'schema_name',d.schema_name,'object_identity',d.object_identity,'in_extension',d.in_extension,
    'columns',(SELECT jsonb_agg(jsonb_build_object(
      'attnum',a.attnum,'name',a.attname,'type',format_type(a.atttypid,a.atttypmod),
      'notnull',a.attnotnull,'dropped',a.attisdropped,'identity',a.attidentity,'generated',a.attgenerated,
      'missing',a.atthasmissing,'missing_value',to_jsonb(a.attmissingval),
      'default',pg_get_expr(ad.adbin,ad.adrelid),
      'attributes',jsonb_build_object('type_oid',a.atttypid,'typmod',a.atttypmod,
        'collation',a.attcollation,'acl',a.attacl,'storage',a.attstorage,
        'compression',a.attcompression,'options',a.attoptions,'fdwoptions',a.attfdwoptions,
        'local',a.attislocal,'inhcount',a.attinhcount)) ORDER BY a.attnum)
      FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
      WHERE d.classid='pg_class'::regclass AND a.attrelid=d.objid AND a.attnum>0),
    'relation',(SELECT jsonb_build_object('kind',c.relkind,'partition',c.relispartition,
      'filenode',c.relfilenode,'name',c.relname,'namespace',c.relnamespace,
      'owner',c.relowner,'acl',c.relacl,'options',c.reloptions,'replica_identity',c.relreplident,
      'tablespace',c.reltablespace,'persistence',c.relpersistence,'access_method',c.relam,
      'row_security',c.relrowsecurity,'force_row_security',c.relforcerowsecurity,
      'partition_key',CASE WHEN c.relkind='p' THEN pg_get_partkeydef(c.oid) END,
      'partition_bound',c.relpartbound::text,
      'constraints',(SELECT coalesce(jsonb_agg(pg_get_constraintdef(k.oid) ORDER BY k.oid),'[]'::jsonb) FROM pg_constraint k WHERE k.conrelid=c.oid),
      'indexes',(SELECT coalesce(jsonb_agg(jsonb_build_array(pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive) ORDER BY i.indexrelid),'[]'::jsonb) FROM pg_index i WHERE i.indrelid=c.oid),
      'triggers',(SELECT coalesce(jsonb_agg(jsonb_build_array(pg_get_triggerdef(t.oid),t.tgenabled) ORDER BY t.oid),'[]'::jsonb) FROM pg_trigger t WHERE t.tgrelid=c.oid),
      'policies',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.oid),'[]'::jsonb) FROM pg_policy p WHERE p.polrelid=c.oid),
      'parents',(SELECT coalesce(jsonb_agg(jsonb_build_array(i.inhparent,i.inhdetachpending) ORDER BY i.inhseqno),'[]'::jsonb) FROM pg_inherits i WHERE i.inhrelid=c.oid))
      FROM pg_class c WHERE d.classid='pg_class'::regclass AND c.oid=d.objid)
    ) ORDER BY d.classid,d.objid,d.objsubid),'[]'::jsonb)
    INTO details FROM pg_event_trigger_ddl_commands() d;
  UPDATE _skedaddle.{name}_version SET version=version+1 WHERE singleton RETURNING version INTO v;
  IF v IS NULL THEN RAISE EXCEPTION 'Skedaddle schema version row missing'; END IF;
  PERFORM pg_logical_emit_message(true,'skedaddle',jsonb_build_object(
    'format',1,'installation','{id}','server_major',{major},
    'origin',nullif(current_setting('skedaddle.admin_origin',true),''),
    'version',v,'tag',TG_TAG,'xid',txid_current()::text,'query',current_query(),'commands',details)::text);
END
"#
        );
        let install = format!(
            r#"BEGIN;
{administrative_origin}
DO $check$ BEGIN IF current_setting('server_version_num')::int / 10000 <> {major} THEN RAISE EXCEPTION 'Skedaddle PG major mismatch'; END IF; END $check$;
DROP EVENT TRIGGER IF EXISTS {name};
CREATE SCHEMA IF NOT EXISTS _skedaddle;
CREATE TABLE IF NOT EXISTS _skedaddle.fence_state(epoch bigint PRIMARY KEY,state text NOT NULL);
CREATE TABLE IF NOT EXISTS _skedaddle.cutover_marker(migration text PRIMARY KEY,cutover text NOT NULL,epoch bigint NOT NULL,lsn pg_lsn NOT NULL);
CREATE TABLE IF NOT EXISTS _skedaddle.cutover_activation(migration text PRIMARY KEY,cutover text NOT NULL,epoch bigint NOT NULL,activated_ms bigint NOT NULL);
ALTER TABLE _skedaddle.cutover_activation ADD COLUMN IF NOT EXISTS fence_owner text, ADD COLUMN IF NOT EXISTS destination_system_identifier text, ADD COLUMN IF NOT EXISTS destination_database text;
CREATE TABLE IF NOT EXISTS _skedaddle.authority(migration text PRIMARY KEY,epoch bigint NOT NULL);
CREATE TABLE IF NOT EXISTS _skedaddle.effects(migration text NOT NULL,epoch bigint NOT NULL,seq bigint NOT NULL,payload text NOT NULL,PRIMARY KEY(migration,epoch,seq));
REVOKE CREATE ON SCHEMA _skedaddle FROM PUBLIC;
CREATE TABLE IF NOT EXISTS _skedaddle.{name}_version(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),version bigint NOT NULL CHECK(version>=0));
REVOKE ALL ON _skedaddle.{name}_version FROM PUBLIC;
INSERT INTO _skedaddle.{name}_version VALUES(true,0) ON CONFLICT(singleton) DO NOTHING;
CREATE OR REPLACE FUNCTION _skedaddle.{name}() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body${body}$body$;
REVOKE ALL ON FUNCTION _skedaddle.{name}() FROM PUBLIC;
CREATE EVENT TRIGGER {name} ON ddl_command_end EXECUTE FUNCTION _skedaddle.{name}();
COMMIT;"#
        );
        let body_literal = body.replace('\'', "''");
        let verify = format!(
            r#"SELECT current_setting('server_version_num')::int / 10000 = {major}
AND EXISTS(SELECT 1 FROM pg_event_trigger e JOIN pg_proc p ON p.oid=e.evtfoid JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE e.evtname='{name}' AND e.evtevent='ddl_command_end' AND e.evtenabled='O' AND e.evttags IS NULL
 AND n.nspname='_skedaddle' AND p.proname='{name}' AND p.prosecdef
 AND p.proconfig=ARRAY['search_path=pg_catalog'] AND p.prosrc='{body_literal}')
AND EXISTS(SELECT 1 FROM _skedaddle.{name}_version WHERE singleton AND version>=0);"#
        );
        let remove = format!(
            "BEGIN; {administrative_origin} DROP EVENT TRIGGER IF EXISTS {name}; DROP FUNCTION IF EXISTS _skedaddle.{name}(); DROP TABLE IF EXISTS _skedaddle.{name}_version; COMMIT;"
        );
        Ok(Self {
            install,
            verify,
            remove,
        })
    }

    /// The journal callback MUST durably persist intent before returning Ok.
    /// Execution is never invoked when journaling fails. Retries preserve the
    /// version row; install/remove are transactional and idempotent.
    pub fn run(
        &self,
        intent: TriggerIntent,
        mut journal: impl FnMut(TriggerIntent, &str) -> Result<(), DdlError>,
        mut execute: impl FnMut(&str) -> Result<(), DdlError>,
    ) -> Result<(), DdlError> {
        let sql = match intent {
            TriggerIntent::Install => &self.install,
            TriggerIntent::Remove => &self.remove,
        };
        journal(intent, sql)?;
        execute(sql)
    }

    /// Async counterpart; intent durability precedes the database operation.
    pub async fn run_async<'a, F: std::future::Future<Output = Result<(), DdlError>>>(
        &'a self,
        intent: TriggerIntent,
        mut journal: impl FnMut(TriggerIntent, &str) -> Result<(), DdlError>,
        execute: impl FnOnce(&'a str) -> F,
    ) -> Result<(), DdlError> {
        let sql = match intent {
            TriggerIntent::Install => &self.install,
            TriggerIntent::Remove => &self.remove,
        };
        journal(intent, sql)?;
        execute(sql).await
    }
}
