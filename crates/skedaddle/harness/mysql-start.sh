#!/bin/bash
# Private CA and server keys never leave the labelled, bounded data volume.
set -euo pipefail
base=/var/lib/mysql/harness
tls=$base/tls
mkdir -p "$tls"
if [ ! -s "$tls/server.pem" ]; then
    umask 077
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -sha256 \
        -subj /CN=skedaddle-test-ca -addext basicConstraints=critical,CA:TRUE \
        -addext keyUsage=critical,keyCertSign,cRLSign \
        -keyout "$tls/ca.key" -out "$tls/ca.pem"
    openssl req -new -newkey rsa:2048 -nodes -subj /CN=localhost \
        -keyout "$tls/server.key" -out "$tls/server.csr"
    printf '%s\n' 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
        'basicConstraints=critical,CA:FALSE' 'extendedKeyUsage=serverAuth' \
        'keyUsage=critical,digitalSignature,keyEncipherment' > "$tls/server.ext"
    openssl x509 -req -in "$tls/server.csr" -CA "$tls/ca.pem" \
        -CAkey "$tls/ca.key" -CAcreateserial -days 2 -sha256 \
        -extfile "$tls/server.ext" -out "$tls/server.pem"
fi
case "${HARNESS_MYSQL_GTID:-ON}" in ON|OFF) ;; *) echo 'GTID must be ON or OFF' >&2; exit 1;; esac
chown mysql:mysql /var/lib/mysql
chown -R mysql:mysql "$base"
exec /usr/local/bin/docker-entrypoint.sh mysqld \
    --datadir="$base/data" --server-id=6401 --log-bin=mysql-bin \
    --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL \
    --gtid-mode="${HARNESS_MYSQL_GTID:-ON}" --enforce-gtid-consistency=ON \
    --ssl-ca="$tls/ca.pem" --ssl-cert="$tls/server.pem" --ssl-key="$tls/server.key" \
    --require-secure-transport=ON --mysqlx=OFF \
    --innodb-buffer-pool-size=32M --innodb-redo-log-capacity=64M \
    --innodb-log-buffer-size=4M --innodb-data-file-path=ibdata1:12M:autoextend:max:64M \
    --innodb-temp-data-file-path=ibtmp1:12M:autoextend:max:32M \
    --max-binlog-size=16M --binlog-expire-logs-seconds=600 \
    --performance-schema=OFF --max-connections=20 --table-open-cache=64 \
    --tmp-table-size=8M --max-heap-table-size=8M "$@"
