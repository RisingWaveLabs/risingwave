/*
 * Copyright 2023 RisingWave Labs
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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.java.binding.Binding;
import com.risingwave.proto.Catalog;
import com.risingwave.proto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresValidator extends DatabaseValidator implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);
    private static final int CDC_TABLE_TYPE =
            Catalog.Table.CdcTableType.CDC_TABLE_TYPE_POSTGRES.getNumber();
    private final Map<String, String> userProps;

    private final TableSchema tableSchema;

    private final Connection jdbcConnection;

    private final String user;
    private final String dbName;
    private final String schemaName;
    private final String tableName;
    private final String pubName;
    private final String slotName;

    private final boolean pubAutoCreate;

    private static final String AWS_RDS_HOST = "amazonaws.com";
    private final boolean isAwsRds;

    // Whether the properties to validate is shared by multiple tables.
    // If true, we will skip validation check for table
    private final boolean isCdcSourceJob;
    private final int pgVersion;

    private static class ColumnInfo {
        String dataType;
        Long charMaxLength;
        String udtName;
        Integer atttypmod;

        ColumnInfo(String dataType, Long charMaxLength, String udtName, Integer atttypmod) {
            this.dataType = dataType;
            this.charMaxLength = charMaxLength;
            this.udtName = udtName;
            this.atttypmod = atttypmod;
        }
    }

    public PostgresValidator(
            Map<String, String> userProps, TableSchema tableSchema, boolean isCdcSourceJob)
            throws SQLException {
        this.userProps = userProps;
        this.tableSchema = tableSchema;

        var dbHost = userProps.get(DbzConnectorConfig.HOST);
        var dbPort = userProps.get(DbzConnectorConfig.PORT);
        var dbName = userProps.get(DbzConnectorConfig.DB_NAME);
        var jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, dbHost, dbPort, dbName);

        var user = userProps.get(DbzConnectorConfig.USER);
        var password = userProps.get(DbzConnectorConfig.PASSWORD);
        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);

        // Determine if this is AWS RDS (priority from high to low):
        // 1. Explicit user configuration via 'postgres.is.aws.rds'
        // 2. Automatic detection via hostname containing 'amazonaws.com'
        // 3. Test-only parameter 'test.only.force.rds' (for backward compatibility)
        this.isAwsRds =
                Boolean.parseBoolean(
                                userProps.getOrDefault(DbzConnectorConfig.PG_IS_AWS_RDS, "false"))
                        || dbHost.contains(AWS_RDS_HOST)
                        || userProps
                                .getOrDefault(DbzConnectorConfig.PG_TEST_ONLY_FORCE_RDS, "false")
                                .equalsIgnoreCase("true");
        this.dbName = dbName;
        this.user = user;
        this.schemaName = userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.pubName = userProps.get(DbzConnectorConfig.PG_PUB_NAME);
        this.slotName = userProps.get(DbzConnectorConfig.PG_SLOT_NAME);

        this.pubAutoCreate =
                userProps.get(DbzConnectorConfig.PG_PUB_CREATE).equalsIgnoreCase("true");
        this.isCdcSourceJob = isCdcSourceJob;
        try {
            this.pgVersion = jdbcConnection.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateDbConfig() {
        try {
            // whenever a newer PG version is released, Debezium will take
            // some time to support it. So even though 18 is not released yet, we put a version
            // guard here.
            LOG.info("Detected upstream Postgres major version: {}", pgVersion);
            if (pgVersion > 18) {
                throw ValidatorUtils.failedPrecondition(
                        "Postgres major version should be less than or equal to 17.");
            }

            try (var stmt = jdbcConnection.createStatement()) {
                // check whether wal has been enabled
                var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.wal"));
                while (res.next()) {
                    if (!res.getString(1).equals("logical")) {
                        throw ValidatorUtils.invalidArgument(
                                "Postgres wal_level should be 'logical'.\nPlease modify the config and restart your Postgres server.");
                    }
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }

        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.slot.check"))) {
            // check whether the replication slot is already existed
            stmt.setString(1, slotName);
            stmt.setString(2, dbName);
            var res = stmt.executeQuery();
            if (res.next() && res.getString(1).equals(slotName)) {
                LOG.info("replication slot '{}' already exists, just use it", slotName);
            } else {
                // otherwise, we need to create a new one
                var stmt2 =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.slot_limit.check"));
                var res2 = stmt2.executeQuery();
                // check whether the number of replication slots reaches the max limit
                if (res2.next() && res2.getString(1).equals("true")) {
                    throw ValidatorUtils.failedPrecondition(
                            "all replication slots are in use\n Hint: Free one or increase max_replication_slots.");
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateUserPrivilege() {
        try {
            validatePrivileges();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateTable() {
        try {
            validateTableSchema();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    /** For Citus which is a distributed version of PG */
    public void validateDistributedTable() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("citus.distributed_table"))) {
            stmt.setString(1, schemaName + "." + tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (!ret.equalsIgnoreCase("distributed")) {
                    throw ValidatorUtils.invalidArgument("Citus table is not a distributed table");
                }
            }
        }
    }

    private void validateTableSchema() throws SQLException {
        if (isCdcSourceJob) {
            return;
        }
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (ret.equalsIgnoreCase("f") || ret.equalsIgnoreCase("false")) {
                    throw ValidatorUtils.invalidArgument("Postgres table or schema doesn't exist");
                }
            }
        }

        // check primary key
        // reference: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.pk"))) {
            stmt.setString(1, String.format("\"%s\".\"%s\"", this.schemaName, this.tableName));
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                pkFields.add(name);
            }
            primaryKeyCheck(tableSchema, pkFields);
        }

        // Check whether source schema match table schema on upstream
        // All columns defined must exist in upstream database
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_schema"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();

            Map<String, ColumnInfo> schema = new HashMap<>();
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                Long charMaxLength =
                        res.getObject(3) == null ? null : ((Number) res.getObject(3)).longValue();
                var udtName = res.getString(4);
                Integer atttypmod =
                        res.getObject(5) == null ? null : ((Number) res.getObject(5)).intValue();
                schema.put(field, new ColumnInfo(dataType, charMaxLength, udtName, atttypmod));
            }

            for (var colDesc : tableSchema.getColumnDescs()) {
                var colName = colDesc.getName();
                // skip validate internal columns
                if (colName.startsWith(ValidatorUtils.INTERNAL_COLUMN_PREFIX)) {
                    continue;
                }
                var colInfo = schema.get(colName);
                if (colInfo == null) {
                    throw ValidatorUtils.invalidArgument(
                            "Column '" + colName + "' not found in the upstream database");
                }
                var expectedType = colDesc.getDataType();
                if (!isDataTypeCompatible(colInfo, expectedType.getTypeName())) {
                    throw ValidatorUtils.invalidArgument(
                            "Incompatible data type of column " + colName);
                }
                var vectorDimError = validateVectorDimension(colInfo, expectedType, colName);
                if (vectorDimError != null) {
                    throw ValidatorUtils.invalidArgument(vectorDimError);
                }
            }
        }
    }

    private static String validateVectorDimension(
            ColumnInfo colInfo, Data.DataType expectedType, String colName) {
        if (expectedType.getTypeName().getNumber() != Data.DataType.TypeName.VECTOR_VALUE) {
            return null;
        }
        if (colInfo.udtName == null || !colInfo.udtName.equalsIgnoreCase("vector")) {
            return null;
        }

        int expectedDim = expectedType.getPrecision();
        // For pgvector, PostgreSQL stores dimension in pg_attribute.atttypmod.
        // `vector(n)` -> atttypmod = n, and dimension-less `vector` -> atttypmod = -1.
        Integer upstreamDim =
                (colInfo.atttypmod != null && colInfo.atttypmod > 0) ? colInfo.atttypmod : null;
        if (upstreamDim == null) {
            return "Incompatible vector dimension of column "
                    + colName
                    + ": upstream vector has no dimension, but expected vector("
                    + expectedDim
                    + ")";
        }
        if (upstreamDim != expectedDim) {
            return "Incompatible vector dimension of column "
                    + colName
                    + ": upstream vector("
                    + upstreamDim
                    + "), but expected vector("
                    + expectedDim
                    + ")";
        }
        return null;
    }

    private static void primaryKeyCheck(TableSchema sourceSchema, Set<String> pkFields)
            throws RuntimeException {
        if (sourceSchema.getPrimaryKeys().size() != pkFields.size()) {
            throw ValidatorUtils.invalidArgument(
                    "Primary key mismatch: the SQL schema defines "
                            + sourceSchema.getPrimaryKeys().size()
                            + " primary key columns, but the source table in Postgres has "
                            + pkFields.size()
                            + " columns.");
        }
        for (var colName : sourceSchema.getPrimaryKeys()) {
            if (!pkFields.contains(colName)) {
                throw ValidatorUtils.invalidArgument(
                        "Primary key mismatch: The primary key list of the source table in Postgres does not contain '"
                                + colName
                                + "'.\nHint: If your primary key contains uppercase letters, please ensure that the primary key in the DML of RisingWave uses the same uppercase format and is wrapped with double quotes (\"\").");
            }
        }
    }

    private void validatePrivileges() throws SQLException {
        boolean isSuperUser = false;
        if (this.isAwsRds) {
            // check privileges for aws rds postgres
            boolean hasReplicationRole = false;
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.rds.role.check"))) {
                stmt.setString(1, this.user);
                stmt.setString(2, this.user);
                var res = stmt.executeQuery();
                while (res.next()) {
                    // check rds_superuser role or rds_replication role is granted
                    isSuperUser = res.getBoolean(1);
                    hasReplicationRole = res.getBoolean(2);
                }
            }

            if (!isSuperUser && !hasReplicationRole) {
                throw ValidatorUtils.invalidArgument(
                        "Postgres user must be superuser or replication role to start walsender.");
            }
        } else {
            // check privileges for standalone postgres
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.superuser.check"))) {
                stmt.setString(1, this.user);
                var res = stmt.executeQuery();
                while (res.next()) {
                    isSuperUser = res.getBoolean(1);
                }
            }
            if (!isSuperUser) {
                // check whether user has replication role
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.role.check"))) {
                    stmt.setString(1, this.user);
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        if (!res.getBoolean(1)) {
                            throw ValidatorUtils.invalidArgument(
                                    "Postgres user must be superuser or replication role to start walsender.");
                        }
                    }
                }
            }
        }

        // check whether select privilege on table for snapshot read
        validateTablePrivileges(isSuperUser);
        validatePublicationConfig(isSuperUser);
    }

    private void validateTablePrivileges(boolean isSuperUser) throws SQLException {
        // cdc source job doesn't have table schema to validate, since its schema is fixed to jsonb
        if (isSuperUser || isCdcSourceJob) {
            return;
        }

        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.table_read_privilege.check"))) {
            stmt.setString(1, this.user);
            stmt.setString(2, String.format("\"%s\".\"%s\"", this.schemaName, this.tableName));
            var res = stmt.executeQuery();
            while (res.next()) {
                if (!res.getBoolean(1)) {
                    throw ValidatorUtils.invalidArgument(
                            "Postgres user must have select privilege on table '"
                                    + schemaName
                                    + "."
                                    + tableName
                                    + "'");
                }
            }
        }
    }

    /* Check required privilege to create/alter a publication */
    private void validatePublicationConfig(boolean isSuperUser) throws SQLException {
        boolean isPublicationCoversTable = false;
        boolean isPublicationExists = false;
        boolean isPartialPublicationEnabled = false;

        // Check whether publication exists
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.publication_exist"))) {
            stmt.setString(1, pubName);
            var res = stmt.executeQuery();
            while (res.next()) {
                isPublicationExists = res.getBoolean(1);
            }
        }

        if (!isPublicationExists && !pubAutoCreate) {
            // We require a publication on upstream to publish table cdc events
            throw ValidatorUtils.invalidArgument(
                    "Publication '" + pubName + "' doesn't exist and auto create is disabled");
        }

        // If the source properties is created by share source, skip the following
        // check of publication
        if (isCdcSourceJob) {
            if (!isPublicationExists) {
                LOG.info(
                        "creating cdc source job: publication '{}' doesn't exist, creating...",
                        pubName);
                DbzSourceUtils.createPostgresPublicationInValidate(userProps);
                LOG.info("creating cdc source job: publication '{}' created successfully", pubName);
            }
            return;
        }

        // When publication exists, we should check whether it covers the table
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.publication_att_exists"));
            while (res.next()) {
                isPartialPublicationEnabled = res.getBoolean(1);
            }
        }

        List<String> partitions = new ArrayList<>();
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.partition_names"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                partitions.add(res.getString(1));
            }
        }

        if (!partitions.isEmpty() && isPublicationExists) {
            // `pubviaroot` in `pg_publication` is added after PG v13, before which PG does not
            // allow adding partitioned table to a publication. So here, if partitions.isEmpty() is
            // false, which means the PG version is >= v13, we can safely check the value of
            // `pubviaroot` of the publication here.
            boolean isPublicationViaRoot = false;
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_pubviaroot"))) {
                stmt.setString(1, pubName);
                var res = stmt.executeQuery();
                if (res.next()) {
                    isPublicationViaRoot = res.getBoolean(1);
                }
            }
            if (!isPublicationViaRoot) {
                // Make sure the publication are created with `publish_via_partition_root = true`,
                // which is required by partitioned tables.
                throw ValidatorUtils.invalidArgument(
                        "Table '"
                                + tableName
                                + "' has partitions, which requires publication '"
                                + pubName
                                + "' to be created with `publish_via_partition_root = true`. \nHint: you can run `SELECT pubviaroot from pg_publication WHERE pubname = '"
                                + pubName
                                + "'` in the upstream Postgres to check.");
            }
        }
        // Only after v13, PG allows adding a partitioned table to a publication. So, if the
        // version is before v13, the tables in a publication are always partition leaves, we don't
        // check their ancestors and descendants anymore.
        if (isPublicationExists && pgVersion >= 13) {
            List<String> family = new ArrayList<>();
            boolean findRoot = false;
            String currentPartition = tableName;
            while (!findRoot) {
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.partition_parent"))) {
                    String schemaPartitionName =
                            String.format("\"%s\".\"%s\"", this.schemaName, currentPartition);
                    stmt.setString(1, schemaPartitionName);
                    stmt.setString(2, schemaPartitionName);
                    stmt.setString(3, schemaPartitionName);
                    var res = stmt.executeQuery();
                    if (res.next()) {
                        String parent = res.getString(1);
                        family.add(parent);
                        currentPartition = parent;
                    } else {
                        findRoot = true;
                    }
                }
            }
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.partition_descendants"))) {
                String schemaTableName =
                        String.format("\"%s\".\"%s\"", this.schemaName, this.tableName);
                stmt.setString(1, schemaTableName);
                stmt.setString(2, schemaTableName);
                var res = stmt.executeQuery();
                while (res.next()) {
                    String descendant = res.getString(1);
                    family.add(descendant);
                }
            }
            // The check here was added based on experimental observations. We found that if a table
            // is added to a publication where its ancestor or descendant is already included, the
            // table cannot be read data from the slot correctly. Therefore, we must verify whether
            // its ancestors or descendants are already in the publication. If yes, we deny the
            // request.
            for (String relative : family) {
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.partition_in_publication.check"))) {
                    stmt.setString(1, schemaName);
                    stmt.setString(2, relative);
                    stmt.setString(3, pubName);
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        if (res.getBoolean(1)) {
                            throw ValidatorUtils.invalidArgument(
                                    String.format(
                                            "The ancestor or descendant partition '%s' of the table partition '%s' is already covered in the publication '%s'. Please use a new publication for '%s'",
                                            relative, tableName, pubName, tableName));
                        }
                    }
                }
            }
        }

        // Query generated columns - they should be excluded from publication validation
        // because PostgreSQL does not replicate generated columns in logical replication
        List<String> generatedColumns = new ArrayList<>();
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.generated_columns"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                generatedColumns.add(res.getString("attname"));
            }
            if (!generatedColumns.isEmpty()) {
                LOG.info(
                        "Found generated columns in table '{}': {}",
                        schemaName + "." + tableName,
                        String.join(", ", generatedColumns));
            }
        } catch (SQLException e) {
            // For PostgreSQL < 12 that doesn't support generated columns, ignore the error
            LOG.debug("Failed to query generated columns (likely PG < 12): {}", e.getMessage());
        }
        // PG 15 and up supports partial publication of table
        // check whether publication covers all columns of the table schema
        if (isPartialPublicationEnabled) {
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_attnames"))) {
                stmt.setString(1, schemaName);
                stmt.setString(2, tableName);
                stmt.setString(3, pubName);
                var res = stmt.executeQuery();
                while (res.next()) {
                    String[] columnsPub = (String[]) res.getArray("attnames").getArray();
                    List<String> attNames = Arrays.asList(columnsPub);
                    for (int i = 0; i < tableSchema.getNumColumns(); i++) {
                        String columnName = tableSchema.getColumnNames()[i];
                        // Skip generated columns - they are not included in publication
                        if (generatedColumns.contains(columnName)) {
                            LOG.info(
                                    "Skipping generated column '{}' in publication validation",
                                    columnName);
                            continue;
                        }
                        if (!attNames.contains(columnName)) {
                            throw ValidatorUtils.invalidArgument(
                                    String.format(
                                            "The publication '%s' does not cover all columns of the table '%s'",
                                            pubName, schemaName + "." + tableName));
                        }
                    }
                    // If we reach here, all non-generated columns are covered
                    isPublicationCoversTable = true;
                    LOG.info(
                            "The publication covers the table '{}'.", schemaName + "." + tableName);
                    break;
                }
            }
        } else {
            // PG <= 14.0
            // check whether the publication covers the subscribed table
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_has_table"))) {
                stmt.setString(1, schemaName);
                stmt.setString(2, tableName);
                stmt.setString(3, pubName);
                var res = stmt.executeQuery();
                if (res.next()) {
                    isPublicationCoversTable = res.getBoolean(1);
                    if (isPublicationCoversTable) {
                        LOG.info(
                                "The publication covers the table '{}'.",
                                schemaName + "." + tableName);
                    }
                }
            }
        }

        // If auto create is enabled and the publication doesn't exist or doesn't cover the table,
        // we need to create or alter the publication. And we need to check the required privileges.
        if (!isPublicationCoversTable) {
            // check whether the user has the CREATE privilege on database
            if (!isSuperUser) {
                validatePublicationPrivileges();
            }
            if (isPublicationExists) {
                alterPublicationIfNeeded();
            } else {
                LOG.info(
                        "Publication '{}' doesn't exist, will be created in the process of streaming job.",
                        this.pubName);
            }
        }
    }

    private void validatePublicationPrivileges() throws SQLException {
        if (isCdcSourceJob) {
            throw ValidatorUtils.invalidArgument(
                    "The connector properties is shared by multiple tables unexpectedly");
        }

        // check whether the user has the CREATE privilege on database
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.database_privilege.check"))) {
            stmt.setString(1, this.user);
            stmt.setString(2, this.dbName);
            stmt.setString(3, this.user);
            var res = stmt.executeQuery();
            while (res.next()) {
                if (!res.getBoolean(1)) {
                    throw ValidatorUtils.invalidArgument(
                            "Postgres user must have create privilege on database '"
                                    + this.dbName
                                    + "'");
                }
            }
        }

        // check whether the user has ownership on the table
        boolean isTableOwner = false;
        String tableOwner = null;
        // check if user is the direct owner of table
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_owner"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                tableOwner = res.getString("tableowner");
                if (tableOwner != null && tableOwner.equals(this.user)) {
                    isTableOwner = true;
                    break;
                }
            }
        }

        // if user is not the direct owner, check if user belongs to an owner group
        if (!isTableOwner && null != tableOwner) {
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.users_of_group"))) {
                stmt.setString(1, tableOwner);
                var res = stmt.executeQuery();
                while (res.next()) {
                    var usersArray = res.getArray("members");
                    if (usersArray == null) {
                        break;
                    }
                    String[] users = (String[]) usersArray.getArray();
                    if (null != users
                            && Arrays.asList(users)
                                    .contains(userProps.get(DbzConnectorConfig.USER))) {
                        isTableOwner = true;
                        break;
                    }
                }
            }
        }
        if (!isTableOwner) {
            throw ValidatorUtils.invalidArgument(
                    "Postgres user must be the owner of table '"
                            + tableName
                            + "' to create/alter publication");
        }
    }

    protected void alterPublicationIfNeeded() throws SQLException {
        if (isCdcSourceJob) {
            throw ValidatorUtils.invalidArgument(
                    "The connector properties is created by a shared source unexpectedly");
        }

        String alterPublicationSql =
                String.format(
                        "ALTER PUBLICATION %s ADD TABLE %s",
                        pubName, String.format("\"%s\".\"%s\"", this.schemaName, this.tableName));
        try (var stmt = jdbcConnection.createStatement()) {
            LOG.info("Altered publication with statement: {}", alterPublicationSql);
            stmt.execute(alterPublicationSql);
        }
    }

    @Override
    public void close() throws Exception {
        if (null != jdbcConnection) {
            jdbcConnection.close();
        }
    }

    private boolean isDataTypeCompatible(ColumnInfo colInfo, Data.DataType.TypeName typeName) {
        LOG.info(
                "Data type compatibility check: PostgreSQL type '{}', colInfo.udtName: {}",
                colInfo.dataType,
                colInfo.udtName);
        return Binding.validateCdcSourceColumnType(
                CDC_TABLE_TYPE,
                colInfo.dataType,
                typeName.getNumber(),
                colInfo.charMaxLength == null ? -1 : colInfo.charMaxLength,
                false,
                colInfo.udtName);
    }
}
