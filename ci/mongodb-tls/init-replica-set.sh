#!/usr/bin/env bash

set -euo pipefail

: "${MONGODB_HOST:?MONGODB_HOST is required}"
: "${MONGODB_REPLICA_SET:?MONGODB_REPLICA_SET is required}"

mongo_args=(
    --quiet
    --host "$MONGODB_HOST"
    --tls
    --tlsCAFile /mongodb-tls/ca.pem
)
if [[ -n "${MONGODB_TLS_CERTIFICATE_KEY_FILE:-}" ]]; then
    mongo_args+=(--tlsCertificateKeyFile "$MONGODB_TLS_CERTIFICATE_KEY_FILE")
fi

for _ in $(seq 1 60); do
    if mongosh "${mongo_args[@]}" --eval 'db.runCommand({ ping: 1 }).ok' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
mongosh "${mongo_args[@]}" --eval 'db.runCommand({ ping: 1 }).ok' >/dev/null

mongosh "${mongo_args[@]}" --eval "
    try {
        rs.initiate({
            _id: '${MONGODB_REPLICA_SET}',
            members: [{ _id: 0, host: '${MONGODB_HOST}:27017' }]
        });
    } catch (error) {
        if (error.code !== 23 && error.codeName !== 'AlreadyInitialized') {
            throw error;
        }
    }
"

for _ in $(seq 1 60); do
    if mongosh "${mongo_args[@]}" --eval \
        'quit(db.hello().isWritablePrimary ? 0 : 1)' >/dev/null 2>&1; then
        exit 0
    fi
    sleep 1
done

echo "MongoDB replica set ${MONGODB_REPLICA_SET} did not elect a primary" >&2
exit 1
