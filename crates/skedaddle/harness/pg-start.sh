#!/bin/sh
# Runtime only: private keys and the extracted OpenSSL CLI stay in this run's
# labeled volume. No apk installation, image build, or host private-key export.
set -eu
base=/var/lib/postgresql/h1
tls=$base/tls
tools=$base/tools
mkdir -p "$tls" "$tools"
if [ ! -s "$tls/server.key" ]; then
    umask 077
    # S1 approach: extract just usr/bin/openssl from this image's Alpine repo.
    repo=$(awk '!/^#/ && /\/main\/?$/ {print; exit}' /etc/apk/repositories)
    case "$repo" in https://*) ;; *) echo 'HTTPS Alpine main repository required' >&2; exit 1;; esac
    arch=$(apk --print-arch)
    wget -q -O "$tools/APKINDEX.tar.gz" "$repo/$arch/APKINDEX.tar.gz"
    tar -xzf "$tools/APKINDEX.tar.gz" -C "$tools" APKINDEX
    version=$(awk 'BEGIN {RS=""; FS="\n"} {p=0; v=""; for(i=1;i<=NF;i++){if($i=="P:openssl")p=1; if($i~/^V:/)v=substr($i,3)} if(p){print v; exit}}' "$tools/APKINDEX")
    test -n "$version"
    url=$repo/$arch/openssl-$version.apk
    printf '%s\n' "$url" > "$tools/openssl-package-url.txt"
    awk 'BEGIN {RS=""} /(^|\n)P:openssl(\n|$)/ {print}' "$tools/APKINDEX" > "$tools/openssl-index-entry.txt"
    wget -q -O "$tools/openssl.apk" "$url"
    tar -xzkf "$tools/openssl.apk" -C "$tools" usr/bin/openssl
    openssl=$tools/usr/bin/openssl
    "$openssl" version
    "$openssl" req -x509 -newkey rsa:2048 -nodes -days 2 -sha256 \
        -subj /CN=skedaddle-test-ca -addext basicConstraints=critical,CA:TRUE \
        -addext keyUsage=critical,keyCertSign,cRLSign \
        -keyout "$tls/ca.key" -out "$tls/ca.pem"
    "$openssl" req -new -newkey rsa:2048 -nodes -subj /CN=localhost \
        -keyout "$tls/server.key" -out "$tls/server.csr"
    printf '%s\n' 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
        'basicConstraints=critical,CA:FALSE' 'extendedKeyUsage=serverAuth' \
        'keyUsage=critical,digitalSignature,keyEncipherment' > "$tls/server.ext"
    "$openssl" x509 -req -in "$tls/server.csr" -CA "$tls/ca.pem" \
        -CAkey "$tls/ca.key" -CAcreateserial -days 2 -sha256 \
        -extfile "$tls/server.ext" -out "$tls/server.pem"
fi
cat > "$base/pg_hba.conf" <<'HBA'
local all all trust
local replication all trust
hostnossl all all all reject
hostnossl replication all all reject
hostssl all all all scram-sha-256
hostssl replication all all scram-sha-256
HBA
chown postgres:postgres /var/lib/postgresql
chown -R postgres:postgres "$base"
chmod 600 "$tls/ca.key" "$tls/server.key"
exec /usr/local/bin/docker-entrypoint.sh postgres \
    -c "wal_level=${HARNESS_WAL_LEVEL:-logical}" -c ssl=on \
    -c "ssl_cert_file=$tls/server.pem" -c "ssl_key_file=$tls/server.key" \
    -c "hba_file=$base/pg_hba.conf" -c password_encryption=scram-sha-256 \
    -c shared_buffers=16MB -c work_mem=1MB -c maintenance_work_mem=16MB \
    -c logical_decoding_work_mem=64kB -c max_connections=20 \
    -c max_replication_slots=8 -c max_wal_senders=8 \
    -c max_prepared_transactions=8 -c max_slot_wal_keep_size=64MB \
    -c max_wal_size=64MB -c min_wal_size=32MB -c wal_keep_size=0 \
    -c temp_file_limit=16MB -c logging_collector=off -c log_statement=none "$@"
