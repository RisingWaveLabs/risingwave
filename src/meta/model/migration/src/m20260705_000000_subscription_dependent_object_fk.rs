use sea_orm_migration::prelude::*;

use crate::m20230908_072257_init::Object;
use crate::sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

const FK_NAME: &str = "FK_subscription_dependent_table_id";
const NEW_TABLE: &str = "subscription_new";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // `dependent_table_id` stores the object oid of the upstream table/MV. Add the missing
        // backend FK so deleting that object cascades stale subscription rows in the meta store.
        let backend = manager.get_database_backend();
        let cleanup_sql = match backend {
            DatabaseBackend::MySql => {
                "DELETE o FROM object AS o \
                 JOIN subscription AS s ON s.subscription_id = o.oid \
                 LEFT JOIN object AS dependent ON dependent.oid = s.dependent_table_id \
                 WHERE dependent.oid IS NULL"
            }
            DatabaseBackend::Postgres => {
                "DELETE FROM object WHERE oid IN (\
                 SELECT subscription_id FROM subscription \
                 WHERE dependent_table_id NOT IN (SELECT oid FROM object))"
            }
            DatabaseBackend::Sqlite => return recreate_table(manager, true).await,
        };
        manager
            .get_connection()
            .execute(Statement::from_string(backend, cleanup_sql))
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Subscription::Table)
                    .add_foreign_key(&dependent_object_foreign_key())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        match manager.get_database_backend() {
            DatabaseBackend::MySql | DatabaseBackend::Postgres => {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Subscription::Table)
                            .drop_foreign_key(Alias::new(FK_NAME))
                            .to_owned(),
                    )
                    .await?;
            }
            DatabaseBackend::Sqlite => {
                recreate_table(manager, false).await?;
            }
        }
        Ok(())
    }
}

fn dependent_object_foreign_key() -> TableForeignKey {
    TableForeignKey::new()
        .name(FK_NAME)
        .from_tbl(Subscription::Table)
        .from_col(Subscription::DependentTableId)
        .to_tbl(Object::Table)
        .to_col(Object::Oid)
        .on_delete(ForeignKeyAction::Cascade)
        .to_owned()
}

async fn recreate_table(manager: &SchemaManager<'_>, with_dependent_fk: bool) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    let txn = manager.get_connection().begin().await?;
    {
        let txn_manager = SchemaManager::new(&txn);

        if with_dependent_fk {
            txn.execute(Statement::from_string(
                backend,
                "DELETE FROM object WHERE oid IN (\
                 SELECT subscription_id FROM subscription \
                 WHERE dependent_table_id NOT IN (SELECT oid FROM object))",
            ))
            .await?;
        }

        let mut create = Table::create();
        create
            .table(Alias::new(NEW_TABLE))
            .col(
                ColumnDef::new(Subscription::SubscriptionId)
                    .integer()
                    .primary_key(),
            )
            .col(ColumnDef::new(Subscription::Name).string().not_null())
            .col(
                ColumnDef::new(Subscription::Definition)
                    .rw_long_text(&txn_manager)
                    .not_null(),
            )
            .col(ColumnDef::new(Subscription::RetentionSeconds).big_integer())
            .col(ColumnDef::new(Subscription::SubscriptionState).integer())
            .col(
                ColumnDef::new(Subscription::DependentTableId)
                    .integer()
                    .not_null(),
            )
            .foreign_key(
                &mut ForeignKey::create()
                    .name("FK_subscription_object_id")
                    .from(Alias::new(NEW_TABLE), Subscription::SubscriptionId)
                    .to(Object::Table, Object::Oid)
                    .on_delete(ForeignKeyAction::Cascade)
                    .to_owned(),
            );
        if with_dependent_fk {
            create.foreign_key(
                &mut ForeignKey::create()
                    .name(FK_NAME)
                    .from(Alias::new(NEW_TABLE), Subscription::DependentTableId)
                    .to(Object::Table, Object::Oid)
                    .on_delete(ForeignKeyAction::Cascade)
                    .to_owned(),
            );
        }
        txn_manager.create_table(create).await?;

        txn.execute(Statement::from_string(
            backend,
            "INSERT INTO subscription_new \
             (subscription_id, name, retention_seconds, definition, subscription_state, dependent_table_id) \
             SELECT subscription_id, name, retention_seconds, definition, subscription_state, dependent_table_id \
             FROM subscription WHERE dependent_table_id IN (SELECT oid FROM object)",
        ))
        .await?;

        txn_manager
            .drop_table(Table::drop().table(Subscription::Table).to_owned())
            .await?;
        txn_manager
            .rename_table(
                Table::rename()
                    .table(Alias::new(NEW_TABLE), Subscription::Table)
                    .to_owned(),
            )
            .await?;
    }
    txn.commit().await?;
    Ok(())
}

#[derive(DeriveIden)]
enum Subscription {
    Table,
    SubscriptionId,
    Name,
    Definition,
    RetentionSeconds,
    SubscriptionState,
    DependentTableId,
}
