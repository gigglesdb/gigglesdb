WITH RECURSIVE
selected AS (
    SELECT c.oid, c.relowner, format('%I.%I',n.nspname,c.relname) AS object
    FROM unnest($1::text[]) AS wanted(name)
    JOIN pg_class c ON c.oid=to_regclass(wanted.name)
    JOIN pg_namespace n ON n.oid=c.relnamespace
),
edges(member,roleid) AS (
    SELECT member,roleid FROM pg_auth_members
    UNION
    SELECT d.datdba,r.oid FROM pg_database d CROSS JOIN pg_roles r
    WHERE d.datname=current_database() AND r.rolname='pg_database_owner'
),
reach(principal,ancestor) AS (
    SELECT oid,oid FROM pg_roles
    UNION
    SELECT reach.principal,m.roleid FROM reach JOIN edges m ON m.member=reach.ancestor
),
capabilities AS (
    SELECT r.oid,r.rolname,s.oid AS table_oid,s.object,
        bool_or(a.rolsuper) AS superuser,
        bool_or(a.rolbypassrls) AS bypass,
        bool_or(a.oid=s.relowner) AS owner,
        bool_or(a.rolsuper OR a.oid=s.relowner OR a.rolname='pg_write_all_data'
            OR has_table_privilege(a.oid,s.oid,'INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(a.oid,s.oid,'INSERT,UPDATE')) AS writer
    FROM pg_roles r JOIN reach ON reach.principal=r.oid
    JOIN pg_roles a ON a.oid=reach.ancestor CROSS JOIN selected s
    GROUP BY r.oid,r.rolname,s.oid,s.object
),
writers AS (SELECT * FROM capabilities WHERE writer),
direct AS (
    SELECT * FROM capabilities WHERE $3::boolean OR (rolname<>current_user AND rolname<>session_user)
),
definers AS (
    SELECT DISTINCT p.oid,pg_get_userbyid(p.proowner) AS principal,
        format('%I.%I(%s)',n.nspname,p.proname,pg_get_function_identity_arguments(p.oid)) AS object
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE p.prosecdef AND EXISTS(SELECT 1 FROM writers w WHERE w.oid=p.proowner)
)
SELECT 'scope'::text,'selected'::text,object,'resolved selected table'::text FROM selected
UNION ALL SELECT 'out-of-scope-writer',rolname,object,'Effective INSERT/UPDATE/DELETE/TRUNCATE, including column ACLs and all membership paths'
    FROM direct WHERE writer AND NOT(rolname=ANY($2::text[]))
UNION ALL SELECT 'superuser',rolname,object,'Superuser reachable; revoking table ACLs cannot fence this role' FROM direct WHERE superuser
UNION ALL SELECT 'table-owner',rolname,object,'Table ownership reachable; revoking table ACLs cannot fence this role' FROM direct WHERE owner
UNION ALL SELECT 'bypass-rls',rolname,object,'BYPASSRLS reachable; row security does not constrain this role' FROM direct WHERE bypass
UNION ALL SELECT
    CASE WHEN EXISTS(SELECT 1 FROM pg_depend d JOIN selected s ON s.oid=d.refobjid
        WHERE d.classid='pg_proc'::regclass AND d.objid=f.oid AND d.refclassid='pg_class'::regclass)
        THEN 'security-definer' ELSE 'unscanned-security-definer' END,
    principal,object,'Writer-owned SECURITY DEFINER; catalog dependencies are partial, body/dynamic SQL not proven safe'
    FROM definers f
UNION ALL SELECT 'unscanned-trigger',pg_get_userbyid(p.proowner),
    format('%I.%I trigger %I',n.nspname,c.relname,t.tgname),
    'Enabled trigger outside selection may write selected tables; includes internal FK cascades; body not scanned'
    FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_proc p ON p.oid=t.tgfoid
    WHERE t.tgenabled<>'D' AND NOT EXISTS(SELECT 1 FROM selected s WHERE s.oid=c.oid)
UNION ALL SELECT DISTINCT 'active-writer-session',w.rolname,w.object,
    'Writer backend exists in current database (including idle sessions); PID is not an acknowledgment identity'
    FROM pg_stat_activity a JOIN writers w ON w.rolname=a.usename
    WHERE a.datname=current_database() AND a.pid<>pg_backend_pid()
UNION ALL SELECT DISTINCT 'prepared-writer',w.rolname,w.object,'Prepared transaction belongs to a writer; resolve with its owner'
    FROM pg_prepared_xacts p JOIN writers w ON w.rolname=p.owner WHERE p.database=current_database()
UNION ALL SELECT 'subscription',pg_get_userbyid(subowner),subname,'Enabled subscription can apply inbound writes; publication relation coverage not proven'
    FROM pg_subscription WHERE subenabled AND subdbid=(SELECT oid FROM pg_database WHERE datname=current_database())
UNION ALL SELECT 'unscanned-inbound-writer','unknown',object,
    'Catalogs cannot exhaustively identify external producers, extension/background workers or future connections; operator review required'
    FROM selected
