select ssl from pg_stat_ssl where pid = pg_backend_pid()
