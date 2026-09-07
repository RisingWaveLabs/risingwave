/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mongodb.connection.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MongoDbTlsUtilsTest {
    private static final String TEST_CA =
            """
            -----BEGIN CERTIFICATE-----
            MIIDKzCCAhOgAwIBAgIUasoPCQmxVsB1pHcu26t1oFXqPwMwDQYJKoZIhvcNAQEL
            BQAwJTEjMCEGA1U
            EAwwaUmlzaW5nV2F2ZSBNb25nb0RCIFRlc3QgQ0EwHhcNMjYw
            ODE4MTAxMTIzWhcNMjYwODE5MTAxMTIzWjAlMSMwIQYDVQQDDBpSaXNpbmdXYXZl
            IE1vbmdvREIgVGVzdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
            AK4WHBdiDKGN+g9VNPT+Q6saCT2273vsBU9y55JybUQac5Yk6rRJMFgBL5L6hr18
            QuYUgTtr7PhCvuzHhnMkIiLxYMKOdwoIPqmi7tvPDhJPcQZV4DOL2HreqQkMAA6o
            6TMUSnF8CBsvFr0mmiJ6IVyt/4wD+ec+7XczaPA+FDzmnj5wLdZ1/OkYxNiVri1g
            NnG41U8iKvpDp1QzV8fHgko3+qXgXvuFG+kLa4HoPGauuOdfUXMpnEHEfBYfljzM
            ayMOTtiYlbMLjvo9CFhegpTB/rtENVUYr8hx+CsJ1Z/iOOJrtWL0OPnz4nzCbpC7
            OMXudPuJM5qYWGaMV8fxz1cCAwEAAaNTMFEwHQYDVR0OBBYEFNg2m6oK6npxoFd7
            5sHMQpdApv1NMB8GA1UdIwQYMB
            aAFNg2m6oK6npxoFd75sHMQpdApv1NMA8GA1Ud
            EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBABwehLFu4Che+Rh0gLuSQg/W
            0o0wQuf6li8jgsAHn9sBUVnl/hS86GhvmXQoBD3UWELclFqCAwPtASXj+3182BEa
            Pd4AG+GHOPF0DDkPJgBalcLgoNHAaDqrTeThaYLAU9vwQGYHPVv19LzEKsPdLIJt
            hJiSERZb9FtZ4pLU8jUkve8WXhyXYgQWnm2uC9f23+OcNLk+ebHPUs0I+/AMCPL5
            h2ep7a9OdjDVWYY1a9dwW279FUBZQUgeZy6fk8LQEOjsY9bmX63ZEV2GESbfoZaQ
            JoXqq9NyqHZofzVRtqCKMu8Kvmcj3CTLib1Sd9azOhNPTOahXfpbwiiftwZIO2I=
            -----END CERTIFICATE-----
            """;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void extractsAndDecodesTlsFileOptions() {
        String connectionString =
                "mongodb://localhost/?tls=true&tlsCAFile=%2Fetc%2Fmongo+certs%2Fca.pem"
                        + "&tlsCertificateKeyFile=%2Fetc%2Fmongo+certs%2Fclient.pem&replicaSet=rs0";
        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);
        assertEquals(Path.of("/etc/mongo certs/ca.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(
                Path.of("/etc/mongo certs/client.pem"),
                tlsFiles.certificateKeyFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?tls=true&replicaSet=rs0",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void extractsSemicolonDelimitedTlsFileOptionsBeforeDecoding() {
        String connectionString =
                "mongodb://localhost/?tlsCAFile=%2Fetc%2Fmongo%2Fca%3Bprod.pem" + ";replicaSet=rs0";

        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);
        assertEquals(Path.of("/etc/mongo/ca;prod.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?replicaSet=rs0",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void handlesCaseInsensitiveTlsFileOptionsAndFragments() {
        String connectionString =
                "mongodb://localhost/?TLSCAFILE=%2Fetc%2Fmongo%2Fca.pem"
                        + "&appName=risingwave&TlsCertificateKeyFile=%2Fetc%2Fmongo%2Fclient.pem"
                        + "#fragment";

        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);
        assertEquals(Path.of("/etc/mongo/ca.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(Path.of("/etc/mongo/client.pem"), tlsFiles.certificateKeyFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?appName=risingwave#fragment",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void rejectsDuplicateTlsFileOptions() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.tlsFiles(
                                        "mongodb://localhost/?tlsCAFile=%2Fca.pem"
                                                + "&TLSCAFILE=%2Fother-ca.pem"));
        assertTrue(error.getMessage().contains("must not be specified more than once"));
    }

    @Test
    public void rejectsEmptyTlsFileOptions() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.tlsFiles(
                                        "mongodb://localhost/?tlsCertificateKeyFile="));
        assertTrue(error.getMessage().contains("must not be empty"));
    }

    @Test
    public void rejectsInvalidPercentEncoding() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> MongoDbTlsUtils.tlsFiles("mongodb://localhost/?tlsCAFile=%ZZ"));
        assertTrue(error.getMessage().contains("Invalid percent-encoding"));
    }

    @Test
    public void createsSslContextFromPem() throws Exception {
        Path caFile = temporaryFolder.newFile("ca.pem").toPath();
        Files.writeString(caFile, TEST_CA);

        var sslContext =
                MongoDbTlsUtils.createTlsSslContext("mongodb://localhost/?tlsCAFile=" + caFile)
                        .orElseThrow();

        assertNotNull(sslContext.getSocketFactory());
        assertTrue(sslContext.getProtocol().startsWith("TLS"));
    }

    @Test
    public void clientFactoryConsumesPemOptionsDirectlyFromConnectionString() throws Exception {
        Path caFile = temporaryFolder.newFile("factory-ca.pem").toPath();
        Files.writeString(caFile, TEST_CA);
        var config =
                Configuration.create()
                        .with(
                                MongoDbConnectorConfig.CONNECTION_STRING,
                                "mongodb://localhost/?replicaSet=rs0&tlsCAFile=" + caFile)
                        .build();

        var settings = new DefaultMongoDbClientFactory(config).getMongoClientSettings();

        assertTrue(settings.getSslSettings().isEnabled());
        assertNotNull(settings.getSslSettings().getContext());
    }

    @Test
    public void rejectsMalformedPem() throws Exception {
        Path caFile = temporaryFolder.newFile("malformed-ca.pem").toPath();
        Files.writeString(
                caFile, "-----BEGIN CERTIFICATE-----\nnot-base64!\n-----END CERTIFICATE-----\n");

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsSslContext(
                                        "mongodb://localhost/?tlsCAFile=" + caFile));

        assertTrue(error.getCause().getMessage().contains("Invalid PEM content"));
    }

    @Test
    public void combinesPemTrustWithDebeziumKeyStore() throws Exception {
        Path caFile = writeCaPem("ca.pem");
        Path keyStore = writeKeyStore("client.p12");
        var connectorConfig = connectorConfig(keyStore, null);

        var managers =
                MongoDbTlsUtils.createTlsManagers(
                        new MongoDbTlsUtils.TlsFiles(Optional.of(caFile), Optional.empty()),
                        connectorConfig);

        assertNotNull(managers.keyManagers());
        assertNotNull(managers.trustManagers());
    }

    @Test
    public void combinesPemKeyWithDebeziumTrustStore() throws Exception {
        Path clientPem = writeClientPem("client.pem");
        Path trustStore = writeTrustStore("trust.p12");
        var connectorConfig = connectorConfig(null, trustStore);

        var managers =
                MongoDbTlsUtils.createTlsManagers(
                        new MongoDbTlsUtils.TlsFiles(Optional.empty(), Optional.of(clientPem)),
                        connectorConfig);

        assertNotNull(managers.keyManagers());
        assertNotNull(managers.trustManagers());
    }

    @Test
    public void createsKeyManagersFromPkcs1RsaPrivateKey() throws Exception {
        var keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        var privateKeyInfo = PrivateKeyInfo.getInstance(keyPair.getPrivate().getEncoded());
        byte[] pkcs1 = privateKeyInfo.parsePrivateKey().toASN1Primitive().getEncoded();
        Path clientPem = writeClientPem("client-pkcs1.pem", "RSA PRIVATE KEY", pkcs1);

        var sslContext =
                MongoDbTlsUtils.createTlsSslContext(
                        "mongodb://localhost/?tls=true",
                        new MongoDbTlsUtils.TlsFiles(Optional.empty(), Optional.of(clientPem)));

        assertNotNull(sslContext.getSocketFactory());
    }

    @Test
    public void rejectsPemAndDebeziumKeyStoreForSameManagerSide() throws Exception {
        Path clientPem = writeClientPem("client.pem");
        Path keyStore = writeKeyStore("client.p12");
        var connectorConfig = connectorConfig(keyStore, null);

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsManagers(
                                        new MongoDbTlsUtils.TlsFiles(
                                                Optional.empty(), Optional.of(clientPem)),
                                        connectorConfig));

        assertTrue(error.getMessage().contains("both tlsCertificateKeyFile"));
    }

    @Test
    public void rejectsPemAndDebeziumTrustStoreForSameManagerSide() throws Exception {
        Path caFile = writeCaPem("ca.pem");
        Path trustStore = writeTrustStore("trust.p12");
        var connectorConfig = connectorConfig(null, trustStore);

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsManagers(
                                        new MongoDbTlsUtils.TlsFiles(
                                                Optional.of(caFile), Optional.empty()),
                                        connectorConfig));

        assertTrue(error.getMessage().contains("both tlsCAFile"));
    }

    @Test
    public void rejectsTlsCaFileWhenTlsIsDisabled() throws Exception {
        Path caFile = temporaryFolder.newFile("ca.pem").toPath();
        Files.writeString(caFile, TEST_CA);

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsSslContext(
                                        "mongodb://localhost/?tls=false&tlsCAFile=" + caFile));
        assertTrue(error.getMessage().contains("TLS is disabled"));
    }

    @Test
    public void rejectsClientCertificatePemWithoutPrivateKey() throws Exception {
        Path certificateKeyFile = temporaryFolder.newFile("client.pem").toPath();
        Files.writeString(certificateKeyFile, TEST_CA);

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsSslContext(
                                        "mongodb://localhost/?tlsCertificateKeyFile="
                                                + certificateKeyFile));
        assertTrue(error.getMessage().contains("private key"));
    }

    private Path writeCaPem(String fileName) throws Exception {
        Path path = temporaryFolder.newFile(fileName).toPath();
        Files.writeString(path, TEST_CA);
        return path;
    }

    private Path writeClientPem(String fileName) throws Exception {
        var keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        return writeClientPem(fileName, "PRIVATE KEY", keyPair.getPrivate().getEncoded());
    }

    private Path writeClientPem(String fileName, String keyLabel, byte[] key) throws Exception {
        String privateKey = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(key);
        Path path = temporaryFolder.newFile(fileName).toPath();
        Files.writeString(
                path,
                TEST_CA
                        + "\n-----BEGIN "
                        + keyLabel
                        + "-----\n"
                        + privateKey
                        + "\n-----END "
                        + keyLabel
                        + "-----\n");
        return path;
    }

    private Path writeKeyStore(String fileName) throws Exception {
        char[] password = "password".toCharArray();
        var keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, password);
        keyStore.setKeyEntry(
                "client", keyPair.getPrivate(), password, new Certificate[] {testCertificate()});
        Path path = temporaryFolder.newFile(fileName).toPath();
        try (var output = Files.newOutputStream(path)) {
            keyStore.store(output, password);
        }
        return path;
    }

    private Path writeTrustStore(String fileName) throws Exception {
        char[] password = "password".toCharArray();
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, password);
        trustStore.setCertificateEntry("ca", testCertificate());
        Path path = temporaryFolder.newFile(fileName).toPath();
        try (var output = Files.newOutputStream(path)) {
            trustStore.store(output, password);
        }
        return path;
    }

    private Certificate testCertificate() throws Exception {
        return CertificateFactory.getInstance("X.509")
                .generateCertificate(
                        new ByteArrayInputStream(TEST_CA.getBytes(StandardCharsets.US_ASCII)));
    }

    private MongoDbConnectorConfig connectorConfig(Path keyStore, Path trustStore) {
        var properties = new Properties();
        properties.setProperty("mongodb.connection.string", "mongodb://localhost/?tls=true");
        properties.setProperty("mongodb.ssl.enabled", "true");
        if (keyStore != null) {
            properties.setProperty("mongodb.ssl.keystore", keyStore.toString());
            properties.setProperty("mongodb.ssl.keystore.password", "password");
            properties.setProperty("mongodb.ssl.keystore.type", "PKCS12");
        }
        if (trustStore != null) {
            properties.setProperty("mongodb.ssl.truststore", trustStore.toString());
            properties.setProperty("mongodb.ssl.truststore.password", "password");
            properties.setProperty("mongodb.ssl.truststore.type", "PKCS12");
        }
        return new MongoDbConnectorConfig(Configuration.from(properties));
    }
}
