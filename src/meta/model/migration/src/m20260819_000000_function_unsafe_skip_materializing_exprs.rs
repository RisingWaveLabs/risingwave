use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .add_column(
                        ColumnDef::new(Function::UnsafeSkipMaterializingExprs)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .drop_column(Function::UnsafeSkipMaterializingExprs)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    UnsafeSkipMaterializingExprs,
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TryGetable};

    use super::*;

    #[tokio::test]
    async fn test_adds_unsafe_skip_materializing_exprs_with_false_default() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"CREATE TABLE "function" ("function_id" INTEGER PRIMARY KEY)"#,
        ))
        .await
        .unwrap();
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"INSERT INTO "function" ("function_id") VALUES (1)"#,
        ))
        .await
        .unwrap();

        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await.unwrap();

        assert!(
            manager
                .has_column("function", "unsafe_skip_materializing_exprs")
                .await
                .unwrap()
        );
        let existing_function = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT CAST("unsafe_skip_materializing_exprs" AS INTEGER) AS "unsafe_skip_materializing_exprs" FROM "function" WHERE "function_id" = 1"#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::try_get(&existing_function, "", "unsafe_skip_materializing_exprs").unwrap(),
            0
        );

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"INSERT INTO "function" ("function_id", "unsafe_skip_materializing_exprs") VALUES (2, TRUE)"#,
        ))
        .await
        .unwrap();
        let opted_out_function = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT CAST("unsafe_skip_materializing_exprs" AS INTEGER) AS "unsafe_skip_materializing_exprs" FROM "function" WHERE "function_id" = 2"#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::try_get(&opted_out_function, "", "unsafe_skip_materializing_exprs").unwrap(),
            1
        );

        Migration.down(&manager).await.unwrap();
        assert!(
            !manager
                .has_column("function", "unsafe_skip_materializing_exprs")
                .await
                .unwrap()
        );
    }
}
