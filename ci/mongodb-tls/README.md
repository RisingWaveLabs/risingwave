# MongoDB CDC TLS CI fixture

This CI fixture supplies two MongoDB replica sets to the inline CDC source lane:

- `mongodb-tls` requires TLS but permits clients without certificates.
- `mongodb-mtls` requires TLS and a client certificate signed by the test CA.

`generate-certs.sh` creates a short-lived CA, server identity, and client identities at
container startup. The files live only in the `mongodb-tls-certs` Docker volume and are
mounted at `/mongodb-tls` in MongoDB and the RisingWave CI container. The server certificate
contains the Docker service names as SANs, so the tests keep hostname verification enabled.

The corresponding SQLLogicTest is
`e2e_test/source_inline/cdc/mongodb/mongodb_tls.slt`.
