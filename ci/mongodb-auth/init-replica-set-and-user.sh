#!/usr/bin/env bash

set -euo pipefail

readonly mongodb_uri='mongodb://root:risingwave@mongodb-auth:27017/admin?directConnection=true'

mongosh --quiet "$mongodb_uri" --eval "
    try {
        rs.initiate({
            _id: 'rs-auth',
            members: [{ _id: 0, host: 'mongodb-auth:27017' }]
        });
    } catch (error) {
        if (error.code !== 23 && error.codeName !== 'AlreadyInitialized') {
            throw error;
        }
    }
"

for _ in $(seq 1 60); do
    if mongosh --quiet "$mongodb_uri" --eval \
        'quit(db.hello().isWritablePrimary ? 0 : 1)' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
mongosh --quiet "$mongodb_uri" --eval \
    'quit(db.hello().isWritablePrimary ? 0 : 1)'

mongosh --quiet "$mongodb_uri" <<'EOF'
const admin = db.getSiblingDB('admin');
const role = {
    role: 'rwListDatabases',
    privileges: [
        { resource: { cluster: true }, actions: ['listDatabases'] },
        { resource: { db: '', collection: '' }, actions: ['find', 'changeStream'] }
    ],
    roles: []
};
if (admin.getRole(role.role) === null) {
    admin.createRole(role);
} else {
    admin.updateRole(role.role, { privileges: role.privileges, roles: role.roles });
}

const user = {
    pwd: 'risingwave',
    roles: [
        { role: 'rwListDatabases', db: 'admin' },
        { role: 'read', db: 'admin' },
        { role: 'read', db: 'rw_allowed' }
    ]
};
if (admin.getUser('rw_limited') === null) {
    admin.createUser({ user: 'rw_limited', ...user });
} else {
    admin.updateUser('rw_limited', user);
}

db.getSiblingDB('rw_allowed').events.updateOne(
    { _id: 1 },
    { $set: { value: 'snapshot' } },
    { upsert: true }
);
EOF
