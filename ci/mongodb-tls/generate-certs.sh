#!/usr/bin/env bash

set -euo pipefail

# Certificates are shared through the CI-only named volume at this path.
cert_dir="${MONGODB_TLS_CERT_DIR:-/mongodb-tls}"

mkdir -p "$cert_dir"

# The CI Docker Compose plugin can start completed dependency services again before
# running the test container. Do not replace certificates already loaded by mongod.
if [ -f "$cert_dir/.ready" ]; then
    openssl verify \
        -CAfile "$cert_dir/ca.pem" \
        "$cert_dir/server.pem" \
        "$cert_dir/client.pem"
    exit 0
fi

rm -f "$cert_dir"/*

openssl genpkey \
    -algorithm RSA \
    -pkeyopt rsa_keygen_bits:2048 \
    -out "$cert_dir/ca.key"
openssl req \
    -x509 \
    -new \
    -sha256 \
    -days 1 \
    -key "$cert_dir/ca.key" \
    -subj "/O=RisingWave CI/CN=MongoDB CDC TLS test CA" \
    -out "$cert_dir/ca.pem"

openssl genpkey \
    -algorithm RSA \
    -pkeyopt rsa_keygen_bits:2048 \
    -out "$cert_dir/server.key"
openssl req \
    -new \
    -sha256 \
    -key "$cert_dir/server.key" \
    -subj "/O=RisingWave CI/CN=mongodb-tls" \
    -out "$cert_dir/server.csr"
cat > "$cert_dir/server.ext" <<'EOF'
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:mongodb-tls,DNS:mongodb-mtls,DNS:localhost,IP:127.0.0.1
EOF
openssl x509 \
    -req \
    -sha256 \
    -days 1 \
    -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.pem" \
    -CAkey "$cert_dir/ca.key" \
    -CAcreateserial \
    -extfile "$cert_dir/server.ext" \
    -out "$cert_dir/server.crt"
cat "$cert_dir/server.crt" "$cert_dir/server.key" > "$cert_dir/server.pem"

openssl genpkey \
    -algorithm RSA \
    -pkeyopt rsa_keygen_bits:2048 \
    -out "$cert_dir/client.key"
openssl req \
    -new \
    -sha256 \
    -key "$cert_dir/client.key" \
    -subj "/O=RisingWave CI/CN=MongoDB CDC TLS client" \
    -out "$cert_dir/client.csr"
cat > "$cert_dir/client.ext" <<'EOF'
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth
EOF
openssl x509 \
    -req \
    -sha256 \
    -days 1 \
    -in "$cert_dir/client.csr" \
    -CA "$cert_dir/ca.pem" \
    -CAkey "$cert_dir/ca.key" \
    -CAcreateserial \
    -extfile "$cert_dir/client.ext" \
    -out "$cert_dir/client.crt"
cat "$cert_dir/client.crt" "$cert_dir/client.key" > "$cert_dir/client.pem"

# OpenSSL 3 emits PKCS#8 keys by default. Convert the same client key to traditional
# RSA PKCS#1 so CI also exercises RisingWave's compatibility conversion path.
openssl rsa \
    -in "$cert_dir/client.key" \
    -traditional \
    -out "$cert_dir/client-rsa-pkcs1.key"
cat \
    "$cert_dir/client.crt" \
    "$cert_dir/client-rsa-pkcs1.key" \
    > "$cert_dir/client-rsa-pkcs1.pem"

openssl verify -CAfile "$cert_dir/ca.pem" "$cert_dir/server.crt" "$cert_dir/client.crt"

# The named volume is private to this CI job. MongoDB must be able to read its server
# identity, while RisingWave must be able to read the two client identity variants.
chmod 644 \
    "$cert_dir/ca.pem" \
    "$cert_dir/server.pem" \
    "$cert_dir/client.pem" \
    "$cert_dir/client-rsa-pkcs1.pem"
rm -f \
    "$cert_dir/ca.key" \
    "$cert_dir/ca.srl" \
    "$cert_dir/server.key" \
    "$cert_dir/server.csr" \
    "$cert_dir/server.crt" \
    "$cert_dir/server.ext" \
    "$cert_dir/client.key" \
    "$cert_dir/client.csr" \
    "$cert_dir/client.crt" \
    "$cert_dir/client.ext" \
    "$cert_dir/client-rsa-pkcs1.key"
touch "$cert_dir/.ready"
