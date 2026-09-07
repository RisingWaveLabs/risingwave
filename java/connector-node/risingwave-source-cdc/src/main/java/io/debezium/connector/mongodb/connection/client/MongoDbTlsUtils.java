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

import com.mongodb.ConnectionString;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/** Utilities for applying MongoDB TLS URI options that are not handled by the Java driver. */
public final class MongoDbTlsUtils {
    private static final String TLS_CA_FILE = "tlsCAFile";
    private static final String TLS_CERTIFICATE_KEY_FILE = "tlsCertificateKeyFile";

    private MongoDbTlsUtils() {}

    /** Paths to the optional trust and client identity PEM files. */
    public record TlsFiles(Optional<Path> caFile, Optional<Path> certificateKeyFile) {
        public boolean isEmpty() {
            return caFile.isEmpty() && certificateKeyFile.isEmpty();
        }
    }

    record TlsManagers(KeyManager[] keyManagers, TrustManager[] trustManagers) {}

    /** Returns the custom CA and client certificate paths from a MongoDB connection string. */
    public static TlsFiles tlsFiles(String connectionString) {
        return new TlsFiles(
                tlsFile(connectionString, TLS_CA_FILE),
                tlsFile(connectionString, TLS_CERTIFICATE_KEY_FILE));
    }

    /**
     * Removes TLS PEM file options, which the Java MongoDB driver does not recognize, from a URI.
     */
    public static String withoutTlsFileOptions(String connectionString) {
        int queryStart = connectionString.indexOf('?');
        if (queryStart < 0) {
            return connectionString;
        }

        int fragmentStart = connectionString.indexOf('#', queryStart + 1);
        String fragment = fragmentStart < 0 ? "" : connectionString.substring(fragmentStart);
        List<String> options = new ArrayList<>();
        for (String option : queryOptions(connectionString)) {
            if (!isOption(option, TLS_CA_FILE) && !isOption(option, TLS_CERTIFICATE_KEY_FILE)) {
                options.add(option);
            }
        }

        String prefix = connectionString.substring(0, queryStart);
        return options.isEmpty()
                ? prefix + fragment
                : prefix + "?" + String.join("&", options) + fragment;
    }

    /** Builds an in-memory TLS context from the PEM files referenced by the connection string. */
    public static Optional<SSLContext> createTlsSslContext(String connectionString) {
        TlsFiles tlsFiles = tlsFiles(connectionString);
        if (tlsFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(createTlsSslContext(withoutTlsFileOptions(connectionString), tlsFiles));
    }

    /** Builds an in-memory TLS context from previously extracted PEM file paths. */
    public static SSLContext createTlsSslContext(String connectionString, TlsFiles tlsFiles) {
        try {
            TrustManager[] trustManagers = null;
            if (tlsFiles.caFile().isPresent()) {
                trustManagers = createTrustManagers(tlsFiles.caFile().orElseThrow());
            }

            KeyManager[] keyManagers = null;
            if (tlsFiles.certificateKeyFile().isPresent()) {
                keyManagers = createKeyManagers(tlsFiles.certificateKeyFile().orElseThrow());
            }

            return createTlsSslContext(
                    connectionString, new TlsManagers(keyManagers, trustManagers));
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException(
                    "Failed to load MongoDB TLS certificate files: " + tlsFiles, e);
        }
    }

    /** Builds a TLS context by selecting PEM or Debezium store configuration per manager side. */
    static SSLContext createTlsSslContext(
            String connectionString, TlsFiles tlsFiles, MongoDbConnectorConfig connectorConfig) {
        try {
            return createTlsSslContext(
                    connectionString, createTlsManagers(tlsFiles, connectorConfig));
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException(
                    "Failed to load MongoDB TLS certificate files: " + tlsFiles, e);
        }
    }

    static TlsManagers createTlsManagers(TlsFiles tlsFiles, MongoDbConnectorConfig connectorConfig)
            throws IOException, GeneralSecurityException {
        if (tlsFiles.certificateKeyFile().isPresent()
                && connectorConfig.getSslKeyStore().isPresent()) {
            throw new IllegalArgumentException(
                    "MongoDB TLS client identity must not be configured with both "
                            + "tlsCertificateKeyFile and mongodb.ssl.keystore");
        }
        if (tlsFiles.caFile().isPresent() && connectorConfig.getSslTrustStore().isPresent()) {
            throw new IllegalArgumentException(
                    "MongoDB TLS trust must not be configured with both "
                            + "tlsCAFile and mongodb.ssl.truststore");
        }

        KeyManager[] keyManagers =
                tlsFiles.certificateKeyFile().isPresent()
                        ? createKeyManagers(tlsFiles.certificateKeyFile().orElseThrow())
                        : createKeyManagers(connectorConfig);
        TrustManager[] trustManagers =
                tlsFiles.caFile().isPresent()
                        ? createTrustManagers(tlsFiles.caFile().orElseThrow())
                        : createTrustManagers(connectorConfig);
        return new TlsManagers(keyManagers, trustManagers);
    }

    private static SSLContext createTlsSslContext(String connectionString, TlsManagers tlsManagers)
            throws GeneralSecurityException {
        ConnectionString parsed = new ConnectionString(connectionString);
        if (Boolean.FALSE.equals(parsed.getSslEnabled())) {
            throw new IllegalArgumentException(
                    "MongoDB TLS certificate file options cannot be set when TLS is disabled");
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(tlsManagers.keyManagers(), tlsManagers.trustManagers(), null);
        return sslContext;
    }

    private static TrustManager[] createTrustManagers(Path caFile)
            throws IOException, GeneralSecurityException {
        List<X509Certificate> certificates = readCertificates(caFile);
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("No PEM-encoded certificates found in " + caFile);
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        int index = 0;
        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry("mongodb-ca-" + index++, certificate);
        }

        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory.getTrustManagers();
    }

    private static KeyManager[] createKeyManagers(Path certificateKeyFile)
            throws IOException, GeneralSecurityException {
        String pem = Files.readString(certificateKeyFile, StandardCharsets.US_ASCII);
        List<X509Certificate> certificates = readCertificates(pem, certificateKeyFile);
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException(
                    "No PEM-encoded client certificates found in " + certificateKeyFile);
        }

        PrivateKey privateKey = readPrivateKey(pem, certificates.get(0), certificateKeyFile);
        char[] password = new char[0];
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry(
                "mongodb-client", privateKey, password, certificates.toArray(Certificate[]::new));

        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password);
        return keyManagerFactory.getKeyManagers();
    }

    private static KeyManager[] createKeyManagers(MongoDbConnectorConfig connectorConfig)
            throws GeneralSecurityException {
        if (connectorConfig.getSslKeyStore().isEmpty()) {
            return null;
        }

        char[] password = connectorConfig.getSslKeyStorePassword();
        KeyStore keyStore =
                MongoDbClientFactory.loadKeyStore(
                        connectorConfig.getSslKeyStoreType(),
                        connectorConfig.getSslKeyStore().orElseThrow(),
                        password);
        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password);
        return keyManagerFactory.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers(MongoDbConnectorConfig connectorConfig)
            throws GeneralSecurityException {
        if (connectorConfig.getSslTrustStore().isEmpty()) {
            return null;
        }

        KeyStore trustStore =
                MongoDbClientFactory.loadKeyStore(
                        connectorConfig.getSslTrustStoreType(),
                        connectorConfig.getSslTrustStore().orElseThrow(),
                        connectorConfig.getSslTrustStorePassword());
        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory.getTrustManagers();
    }

    private static List<X509Certificate> readCertificates(Path path)
            throws IOException, GeneralSecurityException {
        return readCertificates(Files.readString(path, StandardCharsets.US_ASCII), path);
    }

    private static List<X509Certificate> readCertificates(String pem, Path path)
            throws IOException, GeneralSecurityException {
        var converter = new JcaX509CertificateConverter();
        List<X509Certificate> certificates = new ArrayList<>();
        for (Object object : readPemObjects(pem, path)) {
            if (object instanceof X509CertificateHolder certificate) {
                certificates.add(converter.getCertificate(certificate));
            }
        }
        return certificates;
    }

    private static PrivateKey readPrivateKey(
            String pem, X509Certificate certificate, Path certificateKeyFile)
            throws IOException, GeneralSecurityException {
        PrivateKeyInfo privateKeyInfo = null;
        for (Object object : readPemObjects(pem, certificateKeyFile)) {
            if (object instanceof PEMEncryptedKeyPair
                    || object instanceof PKCS8EncryptedPrivateKeyInfo) {
                throw new IllegalArgumentException(
                        "Encrypted MongoDB client private keys are not supported: "
                                + certificateKeyFile);
            }
            if (privateKeyInfo == null && object instanceof PrivateKeyInfo keyInfo) {
                privateKeyInfo = keyInfo;
            }
            if (privateKeyInfo == null && object instanceof PEMKeyPair keyPair) {
                PrivateKeyInfo keyInfo = keyPair.getPrivateKeyInfo();
                if (PKCSObjectIdentifiers.rsaEncryption.equals(
                        keyInfo.getPrivateKeyAlgorithm().getAlgorithm())) {
                    privateKeyInfo = keyInfo;
                }
            }
        }
        if (privateKeyInfo == null) {
            throw new IllegalArgumentException(
                    "No unencrypted PKCS#8 or RSA private key found in " + certificateKeyFile);
        }

        PrivateKey privateKey = new JcaPEMKeyConverter().getPrivateKey(privateKeyInfo);
        if (!privateKey
                .getAlgorithm()
                .equalsIgnoreCase(certificate.getPublicKey().getAlgorithm())) {
            throw new GeneralSecurityException(
                    "MongoDB client private key algorithm does not match its certificate in "
                            + certificateKeyFile);
        }
        return privateKey;
    }

    private static Optional<Path> tlsFile(String connectionString, String optionName) {
        String value = null;
        for (String option : queryOptions(connectionString)) {
            if (!isOption(option, optionName)) {
                continue;
            }
            if (value != null) {
                throw new IllegalArgumentException(
                        "MongoDB connection option '"
                                + optionName
                                + "' must not be specified more than once");
            }
            int separator = option.indexOf('=');
            value = decode(separator < 0 ? "" : option.substring(separator + 1));
        }

        if (value == null) {
            return Optional.empty();
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    "MongoDB connection option '" + optionName + "' must not be empty");
        }
        return Optional.of(Path.of(value));
    }

    private static List<String> queryOptions(String connectionString) {
        int queryStart = connectionString.indexOf('?');
        if (queryStart < 0 || queryStart == connectionString.length() - 1) {
            return List.of();
        }

        String query = connectionString.substring(queryStart + 1);
        int fragmentStart = query.indexOf('#');
        if (fragmentStart >= 0) {
            query = query.substring(0, fragmentStart);
        }
        return splitQueryOptions(query);
    }

    /**
     * Splits a raw MongoDB URI query using the driver's option delimiters and empty-part behavior.
     *
     * <p>Keep this behavior aligned with MongoDB Java driver's {@code
     * ConnectionString.parseOptions}: <a
     * href="https://github.com/mongodb/mongo-java-driver/blob/e34283d11e0624ced3ef60ea11970d16a377d2bd/driver-core/src/main/com/mongodb/ConnectionString.java#L1042-L1066">MongoDB
     * Java Driver 5.2.0 source</a>.
     */
    static List<String> splitQueryOptions(String query) {
        List<String> options = new ArrayList<>();
        for (String option : query.split("&|;")) {
            if (!option.isEmpty()) {
                options.add(option);
            }
        }
        return options;
    }

    private static boolean isOption(String option, String optionName) {
        int separator = option.indexOf('=');
        String key = decode(separator < 0 ? option : option.substring(0, separator));
        return optionName.equalsIgnoreCase(key);
    }

    private static List<Object> readPemObjects(String pem, Path path) throws IOException {
        List<Object> objects = new ArrayList<>();
        try (var parser = new PEMParser(new StringReader(pem))) {
            Object object;
            while ((object = parser.readObject()) != null) {
                objects.add(object);
            }
        } catch (IOException | RuntimeException e) {
            throw new IOException("Invalid PEM content in " + path, e);
        }
        return objects;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid percent-encoding in MongoDB URI option", e);
        }
    }
}
