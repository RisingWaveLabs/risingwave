use sea_orm_migration::prelude::*;

use crate::m20230908_072257_init::Object;
use crate::sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

#[derive(DeriveMigrationName)]
pub struct Migration;

const FK_NAME: &str = "FK_object_belong_to_oid";
const INDEX_NAME: &str = "IDX_object_belong_to_oid";

#[derive(DeriveIden)]
enum ObjectColumn {
    BelongToOid,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        // The migrator records the version only after `up` returns, while MySQL DDL implicitly
        // commits. Guard every DDL step so a new Meta leader can retry a partially run migration.
        if !manager.has_column("object", "belong_to_oid").await? {
            match backend {
                DatabaseBackend::MySql | DatabaseBackend::Postgres => {
                    manager
                        .alter_table(
                            Table::alter()
                                .table(Object::Table)
                                .add_column(
                                    ColumnDef::new(ObjectColumn::BelongToOid).integer().null(),
                                )
                                .to_owned(),
                        )
                        .await?;
                }
                DatabaseBackend::Sqlite => {
                    // SQLite cannot add a foreign key constraint to an existing table separately,
                    // but it allows REFERENCES on a newly added nullable column.
                    manager
                        .get_connection()
                        .execute(Statement::from_string(
                            backend,
                            r#"ALTER TABLE "object" ADD COLUMN "belong_to_oid" INTEGER REFERENCES "object" ("oid") ON DELETE CASCADE"#,
                        ))
                        .await?;
                }
            }
        }

        if !manager.has_index("object", INDEX_NAME).await? {
            manager
                .create_index(
                    Index::create()
                        .name(INDEX_NAME)
                        .table(Object::Table)
                        .col(ObjectColumn::BelongToOid)
                        .to_owned(),
                )
                .await?;
        }

        if matches!(backend, DatabaseBackend::MySql | DatabaseBackend::Postgres)
            && !has_foreign_key(manager).await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Object::Table)
                        .add_foreign_key(
                            TableForeignKey::new()
                                .name(FK_NAME)
                                .from_tbl(Object::Table)
                                .from_col(ObjectColumn::BelongToOid)
                                .to_tbl(Object::Table)
                                .to_col(Object::Oid)
                                .on_delete(ForeignKeyAction::Cascade),
                        )
                        .to_owned(),
                )
                .await?;
        }

        backfill_belong_to_oid(manager).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if matches!(
            manager.get_database_backend(),
            DatabaseBackend::MySql | DatabaseBackend::Postgres
        ) {
            manager
                .alter_table(
                    Table::alter()
                        .table(Object::Table)
                        .drop_foreign_key(Alias::new(FK_NAME))
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_index(
                Index::drop()
                    .name(INDEX_NAME)
                    .table(Object::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Object::Table)
                    .drop_column(ObjectColumn::BelongToOid)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

async fn has_foreign_key(manager: &SchemaManager<'_>) -> Result<bool, DbErr> {
    let backend = manager.get_database_backend();
    let statement = match backend {
        DatabaseBackend::MySql => format!(
            "SELECT 1 FROM information_schema.TABLE_CONSTRAINTS \
             WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'object' \
             AND CONSTRAINT_NAME = '{FK_NAME}' AND CONSTRAINT_TYPE = 'FOREIGN KEY' LIMIT 1"
        ),
        DatabaseBackend::Postgres => format!(
            "SELECT 1 FROM information_schema.table_constraints \
             WHERE constraint_schema = current_schema() AND table_name = 'object' \
             AND constraint_name = '{FK_NAME}' AND constraint_type = 'FOREIGN KEY' LIMIT 1"
        ),
        DatabaseBackend::Sqlite => unreachable!("SQLite defines the foreign key with the column"),
    };
    Ok(manager
        .get_connection()
        .query_one(Statement::from_string(backend, statement))
        .await?
        .is_some())
}

async fn backfill_belong_to_oid(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    // Start with namespace parents, then overwrite them with more specific belong-to relations:
    // internal table -> job and associated source -> table. Indexes and subscriptions are named
    // objects, so they continue to belong to their schemas; their references to upstream tables
    // remain represented by the `index`, `subscription`, and `object_dependency` tables.
    //
    // For implicit Iceberg objects, reconstruct the associations that catalog operations used
    // before `belong_to_oid` was persisted. A sink with the literal `__iceberg_sink_` prefix belongs
    // to the unique Iceberg table in its object dependencies. A generated source belongs to the
    // same-schema Iceberg table
    // identified by its `__iceberg_source_<table>` name, because no source-to-table dependency is
    // persisted.
    let statements = match backend {
        DatabaseBackend::MySql => vec![
            "UPDATE `object` SET `belong_to_oid` = COALESCE(`schema_id`, `database_id`)",
            "UPDATE `object` AS o JOIN `table` AS t ON t.`table_id` = o.`oid` SET o.`belong_to_oid` = t.`belongs_to_job_id` WHERE t.`belongs_to_job_id` IS NOT NULL",
            "UPDATE `object` AS o JOIN `table` AS t ON t.`optional_associated_source_id` = o.`oid` SET o.`belong_to_oid` = t.`table_id`",
            "UPDATE `object` AS o JOIN `sink` AS s ON s.`sink_id` = o.`oid` JOIN (SELECT d.`used_by`, MIN(t.`table_id`) AS `table_id` FROM `object_dependency` AS d JOIN `table` AS t ON t.`table_id` = d.`oid` AND t.`engine` = 'ICEBERG' GROUP BY d.`used_by` HAVING COUNT(*) = 1) AS parent ON parent.`used_by` = o.`oid` SET o.`belong_to_oid` = parent.`table_id` WHERE s.`name` LIKE '!_!_iceberg!_sink!_%' ESCAPE '!'",
            "UPDATE `object` AS o JOIN `source` AS s ON s.`source_id` = o.`oid` JOIN `object` AS parent ON parent.`database_id` = o.`database_id` AND parent.`schema_id` = o.`schema_id` JOIN `table` AS t ON t.`table_id` = parent.`oid` AND t.`engine` = 'ICEBERG' SET o.`belong_to_oid` = parent.`oid` WHERE s.`name` = CONCAT('__iceberg_source_', t.`name`)",
        ],
        DatabaseBackend::Postgres => vec![
            r#"UPDATE "object" SET "belong_to_oid" = COALESCE("schema_id", "database_id")"#,
            r#"UPDATE "object" AS o SET "belong_to_oid" = t."belongs_to_job_id" FROM "table" AS t WHERE t."table_id" = o."oid" AND t."belongs_to_job_id" IS NOT NULL"#,
            r#"UPDATE "object" AS o SET "belong_to_oid" = t."table_id" FROM "table" AS t WHERE t."optional_associated_source_id" = o."oid""#,
            r#"UPDATE "object" AS o SET "belong_to_oid" = parent."table_id" FROM "sink" AS s, (SELECT d."used_by", MIN(t."table_id") AS "table_id" FROM "object_dependency" AS d JOIN "table" AS t ON t."table_id" = d."oid" AND t."engine" = 'ICEBERG' GROUP BY d."used_by" HAVING COUNT(*) = 1) AS parent WHERE s."sink_id" = o."oid" AND parent."used_by" = o."oid" AND s."name" LIKE '!_!_iceberg!_sink!_%' ESCAPE '!'"#,
            r#"UPDATE "object" AS o SET "belong_to_oid" = parent."oid" FROM "source" AS s, "object" AS parent, "table" AS t WHERE s."source_id" = o."oid" AND parent."database_id" = o."database_id" AND parent."schema_id" = o."schema_id" AND t."table_id" = parent."oid" AND t."engine" = 'ICEBERG' AND s."name" = '__iceberg_source_' || t."name""#,
        ],
        DatabaseBackend::Sqlite => vec![
            r#"UPDATE "object" SET "belong_to_oid" = COALESCE("schema_id", "database_id")"#,
            r#"UPDATE "object" SET "belong_to_oid" = (SELECT t."belongs_to_job_id" FROM "table" AS t WHERE t."table_id" = "object"."oid") WHERE EXISTS (SELECT 1 FROM "table" AS t WHERE t."table_id" = "object"."oid" AND t."belongs_to_job_id" IS NOT NULL)"#,
            r#"UPDATE "object" SET "belong_to_oid" = (SELECT t."table_id" FROM "table" AS t WHERE t."optional_associated_source_id" = "object"."oid") WHERE EXISTS (SELECT 1 FROM "table" AS t WHERE t."optional_associated_source_id" = "object"."oid")"#,
            r#"UPDATE "object" SET "belong_to_oid" = (SELECT MIN(t."table_id") FROM "object_dependency" AS d JOIN "table" AS t ON t."table_id" = d."oid" AND t."engine" = 'ICEBERG' WHERE d."used_by" = "object"."oid") WHERE EXISTS (SELECT 1 FROM "sink" AS s WHERE s."sink_id" = "object"."oid" AND s."name" LIKE '!_!_iceberg!_sink!_%' ESCAPE '!') AND (SELECT COUNT(*) FROM "object_dependency" AS d JOIN "table" AS t ON t."table_id" = d."oid" AND t."engine" = 'ICEBERG' WHERE d."used_by" = "object"."oid") = 1"#,
            r#"UPDATE "object" SET "belong_to_oid" = (SELECT parent."oid" FROM "source" AS s JOIN "object" AS parent ON parent."database_id" = "object"."database_id" AND parent."schema_id" = "object"."schema_id" JOIN "table" AS t ON t."table_id" = parent."oid" AND t."engine" = 'ICEBERG' WHERE s."source_id" = "object"."oid" AND s."name" = '__iceberg_source_' || t."name") WHERE EXISTS (SELECT 1 FROM "source" AS s JOIN "object" AS parent ON parent."database_id" = "object"."database_id" AND parent."schema_id" = "object"."schema_id" JOIN "table" AS t ON t."table_id" = parent."oid" AND t."engine" = 'ICEBERG' WHERE s."source_id" = "object"."oid" AND s."name" = '__iceberg_source_' || t."name")"#,
        ],
    };

    for statement in statements {
        manager
            .get_connection()
            .execute(Statement::from_string(backend, statement))
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use sea_orm::{Database, TryGetable};

    use super::*;

    #[tokio::test]
    async fn test_sqlite_backfill_and_cascade() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        for sql in [
            r#"CREATE TABLE "object" ("oid" INTEGER PRIMARY KEY, "schema_id" INTEGER, "database_id" INTEGER)"#,
            r#"CREATE TABLE "table" ("table_id" INTEGER PRIMARY KEY, "name" TEXT, "engine" TEXT, "belongs_to_job_id" INTEGER, "optional_associated_source_id" INTEGER)"#,
            r#"CREATE TABLE "index" ("index_id" INTEGER PRIMARY KEY, "primary_table_id" INTEGER)"#,
            r#"CREATE TABLE "subscription" ("subscription_id" INTEGER PRIMARY KEY, "dependent_table_id" INTEGER)"#,
            r#"CREATE TABLE "sink" ("sink_id" INTEGER PRIMARY KEY, "name" TEXT)"#,
            r#"CREATE TABLE "source" ("source_id" INTEGER PRIMARY KEY, "name" TEXT)"#,
            r#"CREATE TABLE "object_dependency" ("oid" INTEGER, "used_by" INTEGER)"#,
            r#"INSERT INTO "object" ("oid", "schema_id", "database_id") VALUES (1, NULL, NULL), (2, NULL, 1), (3, 2, 1), (4, 2, 1), (5, 2, 1), (6, 2, 1), (7, 2, 1), (8, 2, 1), (9, 2, 1), (10, 2, 1), (11, 2, 1), (12, 2, 1)"#,
            r#"INSERT INTO "table" ("table_id", "name", "engine", "belongs_to_job_id", "optional_associated_source_id") VALUES (3, 'iceberg_table', 'ICEBERG', NULL, 5), (4, 'table_internal', 'HUMMOCK', 3, NULL), (8, 'sink_internal', 'HUMMOCK', 6, NULL), (10, 'index_internal', 'HUMMOCK', 9, NULL)"#,
            r#"INSERT INTO "index" ("index_id", "primary_table_id") VALUES (9, 3)"#,
            r#"INSERT INTO "subscription" ("subscription_id", "dependent_table_id") VALUES (11, 3)"#,
            r#"INSERT INTO "source" ("source_id", "name") VALUES (5, 'table_source'), (7, '__iceberg_source_iceberg_table')"#,
            r#"INSERT INTO "sink" ("sink_id", "name") VALUES (6, '__iceberg_sink_renamed'), (12, 'myiceberg_sink_output')"#,
            r#"INSERT INTO "object_dependency" ("oid", "used_by") VALUES (3, 6), (3, 12)"#,
        ] {
            db.execute(Statement::from_string(DatabaseBackend::Sqlite, sql))
                .await
                .unwrap();
        }

        Migration.up(&SchemaManager::new(&db)).await.unwrap();

        let rows = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT "oid", "belong_to_oid" FROM "object" ORDER BY "oid""#,
            ))
            .await
            .unwrap();
        let belong_to_oids = rows
            .into_iter()
            .map(|row| {
                (
                    i32::try_get(&row, "", "oid").unwrap(),
                    Option::<i32>::try_get(&row, "", "belong_to_oid").unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            belong_to_oids,
            vec![
                (1, None),
                (2, Some(1)),
                (3, Some(2)),
                (4, Some(3)),
                (5, Some(3)),
                (6, Some(3)),
                (7, Some(3)),
                (8, Some(6)),
                (9, Some(2)),
                (10, Some(9)),
                (11, Some(2)),
                (12, Some(2)),
            ]
        );

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"DELETE FROM "object" WHERE "oid" = 3"#,
        ))
        .await
        .unwrap();
        let remaining = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT COUNT(*) AS "count" FROM "object" WHERE "oid" IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 12)"#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(i64::try_get(&remaining, "", "count").unwrap(), 4);
    }

    #[tokio::test]
    async fn test_sqlite_partial_run_retry() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        for sql in [
            r#"CREATE TABLE "object" ("oid" INTEGER PRIMARY KEY, "schema_id" INTEGER, "database_id" INTEGER)"#,
            r#"CREATE TABLE "table" ("table_id" INTEGER PRIMARY KEY, "name" TEXT, "engine" TEXT, "belongs_to_job_id" INTEGER, "optional_associated_source_id" INTEGER)"#,
            r#"CREATE TABLE "sink" ("sink_id" INTEGER PRIMARY KEY, "name" TEXT)"#,
            r#"CREATE TABLE "source" ("source_id" INTEGER PRIMARY KEY, "name" TEXT)"#,
            r#"CREATE TABLE "object_dependency" ("oid" INTEGER, "used_by" INTEGER)"#,
            r#"INSERT INTO "object" ("oid", "schema_id", "database_id") VALUES (1, NULL, NULL), (2, NULL, 1), (3, 2, 1), (4, 2, 1)"#,
            r#"INSERT INTO "table" ("table_id", "name", "engine", "belongs_to_job_id", "optional_associated_source_id") VALUES (3, 'table', 'HUMMOCK', NULL, NULL), (4, 'internal', 'HUMMOCK', 3, NULL)"#,
            // Simulate Meta exiting after the first DDL statement but before recording the
            // migration version.
            r#"ALTER TABLE "object" ADD COLUMN "belong_to_oid" INTEGER REFERENCES "object" ("oid") ON DELETE CASCADE"#,
        ] {
            db.execute(Statement::from_string(DatabaseBackend::Sqlite, sql))
                .await
                .unwrap();
        }

        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await.unwrap();
        // Simulate another exit after all migration statements but before recording the version.
        Migration.up(&manager).await.unwrap();

        assert!(manager.has_column("object", "belong_to_oid").await.unwrap());
        assert!(manager.has_index("object", INDEX_NAME).await.unwrap());
        let internal = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT "belong_to_oid" FROM "object" WHERE "oid" = 4"#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(i32::try_get(&internal, "", "belong_to_oid").unwrap(), 3);

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"DELETE FROM "object" WHERE "oid" = 3"#,
        ))
        .await
        .unwrap();
        assert!(
            db.query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT "oid" FROM "object" WHERE "oid" = 4"#,
            ))
            .await
            .unwrap()
            .is_none()
        );
    }
}
