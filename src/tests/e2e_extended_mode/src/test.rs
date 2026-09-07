// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pg_interval::Interval;
use rust_decimal::Decimal;
use tokio::select;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, Row};

pub struct TestSuite {
    config: String,
}

macro_rules! test_eq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    return Err(anyhow!(
                        "{}:{} assertion failed: `(left == right)` \
                                (left: `{:?}`, right: `{:?}`)",
                        file!(),
                        line!(),
                        left_val,
                        right_val
                    ));
                }
            }
        }
    };
}

impl TestSuite {
    pub fn new(
        db_name: String,
        user_name: String,
        server_host: String,
        server_port: u16,
        password: String,
    ) -> Self {
        let config = if !password.is_empty() {
            format!(
                "dbname={} user={} host={} port={} password={}",
                db_name, user_name, server_host, server_port, password
            )
        } else {
            format!(
                "dbname={} user={} host={} port={}",
                db_name, user_name, server_host, server_port
            )
        };
        Self { config }
    }

    fn init_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .try_init();
    }

    pub async fn test(&self) -> anyhow::Result<()> {
        Self::init_logger();
        self.binary_param_and_result().await?;
        self.dql_dml_with_param().await?;
        self.max_row().await?;
        self.multiple_on_going_portal().await?;
        self.create_with_parameter().await?;
        self.simple_cancel(false).await?;
        self.simple_cancel(true).await?;
        self.complex_cancel(false).await?;
        self.complex_cancel(true).await?;
        self.query_cursor_fetch_cancel(false).await?;
        self.query_cursor_fetch_cancel(true).await?;
        self.subscription_fetch_cancel(false).await?;
        self.subscription_fetch_cancel(true).await?;
        self.cursor_fetch_timeout_cancel_and_resume(false).await?;
        self.cursor_fetch_timeout_cancel_and_resume(true).await?;
        self.cursor_queries_survive_statement_timeout(false).await?;
        self.cursor_queries_survive_statement_timeout(true).await?;
        self.subquery_with_param().await?;
        self.create_mview_with_parameter().await?;
        Ok(())
    }

    async fn create_client(&self, is_distributed: bool) -> anyhow::Result<Client> {
        let (client, connection) = tokio_postgres::connect(&self.config, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        if is_distributed {
            client.execute("set query_mode = distributed", &[]).await?;
        } else {
            client.execute("set query_mode = local", &[]).await?;
        }

        Ok(client)
    }

    pub async fn binary_param_and_result(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        for row in client.query("select $1::SMALLINT;", &[&1024_i16]).await? {
            let data: i16 = row.try_get(0)?;
            test_eq!(data, 1024);
        }

        for row in client.query("select $1::INT;", &[&144232_i32]).await? {
            let data: i32 = row.try_get(0)?;
            test_eq!(data, 144232);
        }

        for row in client.query("select $1::BIGINT;", &[&99999999_i64]).await? {
            let data: i64 = row.try_get(0)?;
            test_eq!(data, 99999999);
        }

        for row in client
            .query(
                "select $1::DECIMAL;",
                &[&Decimal::try_from(2.33454_f32).ok()],
            )
            .await?
        {
            let data: Decimal = row.try_get(0)?;
            test_eq!(data, Decimal::try_from(2.33454_f32).unwrap());
        }

        for row in client.query("select $1::BOOL;", &[&true]).await? {
            let data: bool = row.try_get(0)?;
            assert!(data);
        }

        for row in client.query("select $1::REAL;", &[&1.234234_f32]).await? {
            let data: f32 = row.try_get(0)?;
            test_eq!(data, 1.234234);
        }

        for row in client
            .query("select $1::DOUBLE PRECISION;", &[&234234.23490238483_f64])
            .await?
        {
            let data: f64 = row.try_get(0)?;
            test_eq!(data, 234234.23490238483);
        }

        for row in client
            .query(
                "select $1::date;",
                &[&NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()],
            )
            .await?
        {
            let data: NaiveDate = row.try_get(0)?;
            test_eq!(data, NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
        }

        for row in client
            .query(
                "select $1::time",
                &[&NaiveTime::from_hms_opt(10, 0, 0).unwrap()],
            )
            .await?
        {
            let data: NaiveTime = row.try_get(0)?;
            test_eq!(data, NaiveTime::from_hms_opt(10, 0, 0).unwrap());
        }

        for row in client
            .query(
                "select $1::timestamp",
                &[&NaiveDate::from_ymd_opt(2022, 1, 1)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap()],
            )
            .await?
        {
            let data: NaiveDateTime = row.try_get(0)?;
            test_eq!(
                data,
                NaiveDate::from_ymd_opt(2022, 1, 1)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap()
            );
        }

        let timestamptz = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2022, 1, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        for row in client
            .query("select $1::timestamptz", &[&timestamptz])
            .await?
        {
            let data: DateTime<Utc> = row.try_get(0)?;
            test_eq!(data, timestamptz);
        }

        for row in client
            .query("select $1::interval", &[&Interval::new(1, 1, 24000000)])
            .await?
        {
            let data: Interval = row.try_get(0)?;
            test_eq!(data, Interval::new(1, 1, 24000000));
        }

        Ok(())
    }

    async fn dql_dml_with_param(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        client.query("create table t(id int)", &[]).await?;

        let insert_statement = client
            .prepare_typed("insert INTO t (id) VALUES ($1)", &[])
            .await?;

        for i in 0..20 {
            client.execute(&insert_statement, &[&i]).await?;
        }
        client.execute("flush", &[]).await?;

        let update_statement = client
            .prepare_typed(
                "update t set id = $1 where id < $2",
                &[Type::INT4, Type::INT4],
            )
            .await?;
        let query_statement = client
            .prepare_typed(
                "select * FROM t where id < $1 order by id ASC",
                &[Type::INT4],
            )
            .await?;
        let delete_statement = client
            .prepare_typed("delete FROM t where id < $1", &[Type::INT4])
            .await?;

        let mut i = 0;
        for row in client.query(&query_statement, &[&10_i32]).await? {
            let id: i32 = row.try_get(0)?;
            test_eq!(id, i);
            i += 1;
        }
        test_eq!(i, 10);

        client
            .execute(&update_statement, &[&100_i32, &10_i32])
            .await?;
        client.execute("flush", &[]).await?;

        let mut i = 0;
        for _ in client.query(&query_statement, &[&10_i32]).await? {
            i += 1;
        }
        test_eq!(i, 0);

        client.execute(&delete_statement, &[&20_i32]).await?;
        client.execute("flush", &[]).await?;

        let mut i = 0;
        for row in client.query(&query_statement, &[&101_i32]).await? {
            let id: i32 = row.try_get(0)?;
            test_eq!(id, 100);
            i += 1;
        }
        test_eq!(i, 10);

        client.execute("drop table t", &[]).await?;

        Ok(())
    }

    async fn max_row(&self) -> anyhow::Result<()> {
        let mut client = self.create_client(false).await?;

        client.query("create table t(id int)", &[]).await?;

        let insert_statement = client
            .prepare_typed("insert INTO t (id) VALUES ($1)", &[])
            .await?;

        for i in 0..10 {
            client.execute(&insert_statement, &[&i]).await?;
        }
        client.execute("flush", &[]).await?;

        let transaction = client.transaction().await?;
        let statement = transaction
            .prepare_typed("SELECT * FROM t order by id", &[])
            .await?;
        let portal = transaction.bind(&statement, &[]).await?;

        for t in 0..5 {
            let rows = transaction.query_portal(&portal, 1).await?;
            test_eq!(rows.len(), 1);
            let row = rows.first().unwrap();
            let id: i32 = row.get(0);
            test_eq!(id, t);
        }

        let mut i = 5;
        for row in transaction.query_portal(&portal, 3).await? {
            let id: i32 = row.get(0);
            test_eq!(id, i);
            i += 1;
        }
        test_eq!(i, 8);

        for row in transaction.query_portal(&portal, 5).await? {
            let id: i32 = row.get(0);
            test_eq!(id, i);
            i += 1;
        }
        test_eq!(i, 10);

        transaction.rollback().await?;

        client.execute("drop table t", &[]).await?;

        Ok(())
    }

    async fn multiple_on_going_portal(&self) -> anyhow::Result<()> {
        let mut client = self.create_client(false).await?;

        let transaction = client.transaction().await?;
        let statement = transaction
            .prepare_typed("SELECT generate_series(1,5,1)", &[])
            .await?;
        let portal_1 = transaction.bind(&statement, &[]).await?;
        let portal_2 = transaction.bind(&statement, &[]).await?;

        let rows = transaction.query_portal(&portal_1, 1).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.first().unwrap().get::<usize, i32>(0), 1);

        let rows = transaction.query_portal(&portal_2, 1).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.first().unwrap().get::<usize, i32>(0), 1);

        let rows = transaction.query_portal(&portal_2, 3).await?;
        test_eq!(rows.len(), 3);
        test_eq!(rows.first().unwrap().get::<usize, i32>(0), 2);
        test_eq!(rows.get(1).unwrap().get::<usize, i32>(0), 3);
        test_eq!(rows.get(2).unwrap().get::<usize, i32>(0), 4);

        let rows = transaction.query_portal(&portal_1, 1).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.first().unwrap().get::<usize, i32>(0), 2);

        Ok(())
    }

    // Can't support these sql
    async fn create_with_parameter(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        test_eq!(
            client
                .query("create table t as select $1", &[])
                .await
                .is_err(),
            true
        );
        test_eq!(
            client
                .query("create view v as select $1", &[])
                .await
                .is_err(),
            true
        );

        Ok(())
    }

    async fn create_mview_with_parameter(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        let statement = client
            .prepare_typed(
                "create materialized view mv as select $1 as x",
                &[Type::INT4],
            )
            .await?;

        client.execute(&statement, &[&42_i32]).await?;

        let rows = client.query("select * from mv", &[]).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.first().unwrap().get::<usize, i32>(0), 42);

        // Test renaming mv because it relies on parsing and rewrite the `create MV` query
        client
            .execute("alter materialized view mv rename to mv2", &[])
            .await?;

        let rows = client.query("select * from mv2", &[]).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.first().unwrap().get::<usize, i32>(0), 42);

        client.execute("drop materialized view mv2", &[]).await?;

        Ok(())
    }

    async fn simple_cancel(&self, is_distributed: bool) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;
        client.execute("create table t(id int)", &[]).await?;

        let insert_statement = client
            .prepare_typed("insert INTO t (id) VALUES ($1)", &[])
            .await?;

        for i in 0..1000 {
            client.execute(&insert_statement, &[&i]).await?;
        }

        client.execute("flush", &[]).await?;

        let cancel_token = client.cancel_token();

        let query_handle = tokio::spawn(async move {
            client.query("select * from t", &[]).await.unwrap();
        });

        select! {
            _ = query_handle => {
                tracing::error!("Failed to cancel query")
            },
            _ = cancel_token.cancel_query(NoTls) => {
                tracing::trace!("Cancel query successfully")
            },
        }

        let new_client = self.create_client(is_distributed).await?;

        let rows = new_client
            .query("select * from t order by id limit 10", &[])
            .await?;

        test_eq!(rows.len(), 10);
        for (expect_id, row) in rows.iter().enumerate() {
            let id: i32 = row.get(0);
            test_eq!(id, expect_id as i32);
        }

        new_client.execute("drop table t", &[]).await?;

        Ok(())
    }

    async fn complex_cancel(&self, is_distributed: bool) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;

        client
            .execute("create table t1(name varchar, id int)", &[])
            .await?;
        client
            .execute("create table t2(name varchar, id int)", &[])
            .await?;
        client
            .execute("create table t3(name varchar, id int)", &[])
            .await?;

        let insert_statement = client
            .prepare_typed("insert INTO t1 (name, id) VALUES ($1, $2)", &[])
            .await?;
        let insert_statement2 = client
            .prepare_typed("insert INTO t2 (name, id) VALUES ($1, $2)", &[])
            .await?;
        let insert_statement3 = client
            .prepare_typed("insert INTO t3 (name, id) VALUES ($1, $2)", &[])
            .await?;
        for i in 0..1000 {
            client
                .execute(&insert_statement, &[&i.to_string(), &i])
                .await?;
            client
                .execute(&insert_statement2, &[&i.to_string(), &i])
                .await?;
            client
                .execute(&insert_statement3, &[&i.to_string(), &i])
                .await?;
        }

        client.execute("flush", &[]).await?;

        client.execute("set query_mode=local", &[]).await?;

        let cancel_token = client.cancel_token();

        let query_sql = "SELECT t1.name, t2.id, t3.name
        FROM t1
        INNER JOIN (
          SELECT id, name
          FROM t2
          WHERE id IN (
            SELECT id
            FROM t1
            WHERE name LIKE '%1%'
          )
        ) AS t2 ON t1.id = t2.id
        LEFT JOIN t3 ON t2.name = t3.name
        WHERE t3.id IN (
          SELECT MAX(id)
          FROM t3
          GROUP BY name
        )
        ORDER BY t1.name ASC, t3.id DESC
        ";

        let query_handle = tokio::spawn(async move {
            let result = client.query(query_sql, &[]).await;
            match result {
                Ok(_) => {
                    tracing::error!("Query should be canceled");
                }
                Err(e) => {
                    tracing::error!("Query failed with error: {:?}", e);
                }
            };
        });

        select! {
            _ = query_handle => {
                tracing::error!("Failed to cancel query")
            },
            _ = cancel_token.cancel_query(NoTls) => {
                tracing::info!("Cancel query successfully")
            },
        }

        let new_client = self.create_client(is_distributed).await?;

        let rows = new_client
            .query(&format!("{} LIMIT 10", query_sql), &[])
            .await?;
        let expect_ans = [
            (1, 1, 1),
            (10, 10, 10),
            (100, 100, 100),
            (101, 101, 101),
            (102, 102, 102),
            (103, 103, 103),
            (104, 104, 104),
            (105, 105, 105),
            (106, 106, 106),
            (107, 107, 107),
        ];
        for (i, row) in rows.iter().enumerate() {
            test_eq!(
                row.get::<_, String>(0).parse::<i32>().unwrap(),
                expect_ans[i].0
            );
            test_eq!(row.get::<_, i32>(1), expect_ans[i].1);
            test_eq!(
                row.get::<_, String>(2).parse::<i32>().unwrap(),
                expect_ans[i].2
            );
        }

        new_client.execute("drop table t1", &[]).await?;
        new_client.execute("drop table t2", &[]).await?;
        new_client.execute("drop table t3", &[]).await?;
        Ok(())
    }

    fn assert_contiguous_ids(
        rows: &[Row],
        first_id: i32,
        expected_len: usize,
    ) -> anyhow::Result<()> {
        test_eq!(rows.len(), expected_len);
        for (offset, row) in rows.iter().enumerate() {
            test_eq!(row.get::<_, i32>(0), first_id + offset as i32);
        }
        Ok(())
    }

    async fn subscription_fetch_cancel(&self, is_distributed: bool) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;
        let suffix = if is_distributed { "dist" } else { "local" };
        let table_name = format!("sub_cancel_t_{suffix}");
        let subscription_name = format!("sub_cancel_{suffix}");

        client
            .execute(&format!("create table {table_name}(v int)"), &[])
            .await?;
        client
            .execute(
                &format!("create subscription {subscription_name} from {table_name} with(retention = '1D')"),
                &[],
            )
            .await?;
        client
            .execute(
                &format!("declare cur subscription cursor for {subscription_name} since now()"),
                &[],
            )
            .await?;

        let cancel_token = client.cancel_token();
        let fetch_handle = tokio::spawn(async move {
            let result = client
                .query("fetch 1 from cur with (timeout = '60s')", &[])
                .await;
            (client, result)
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        cancel_token.cancel_query(NoTls).await?;

        let (client, result) =
            tokio::time::timeout(Duration::from_secs(10), fetch_handle).await??;
        if result.is_ok() {
            return Err(anyhow!(
                "subscription cursor fetch should be cancelled by CancelRequest"
            ));
        }

        let producer = self.create_client(is_distributed).await?;
        producer
            .execute(&format!("insert into {table_name} values (1)"), &[])
            .await?;
        producer.execute("flush", &[]).await?;

        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            client.query("fetch 1 from cur with (timeout = '10s')", &[]),
        )
        .await??;
        Self::assert_contiguous_ids(&rows, 1, 1)?;

        let rows = tokio::time::timeout(
            Duration::from_secs(2),
            client.query("fetch 1 from cur", &[]),
        )
        .await??;
        test_eq!(rows.len(), 0);

        client.execute("close cur", &[]).await?;
        client
            .execute(&format!("drop subscription {subscription_name}"), &[])
            .await?;
        client
            .execute(&format!("drop table {table_name}"), &[])
            .await?;
        Ok(())
    }

    /// Verifies a positive query-cursor `FETCH` timeout, then subscription cancellation after
    /// crossing an epoch boundary and immediate resumption from available cursor progress.
    async fn cursor_fetch_timeout_cancel_and_resume(
        &self,
        is_distributed: bool,
    ) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;

        client
            .execute(
                "declare fetch_timeout_cur cursor for \
                 select 1::int as id, pg_sleep(0) \
                 union all \
                 select 2::int, pg_sleep(5)",
                &[],
            )
            .await?;
        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            client.query("fetch 10 from fetch_timeout_cur with (timeout = '1s')", &[]),
        )
        .await??;
        // The per-`FETCH` timeout returns the first completed branch promptly instead of waiting
        // five seconds for the cursor query's second branch.
        Self::assert_contiguous_ids(&rows, 1, 1)?;
        client.execute("close fetch_timeout_cur", &[]).await?;

        let producer = self.create_client(is_distributed).await?;
        let suffix = if is_distributed { "dist" } else { "local" };
        let table_name = format!("sub_progress_t_{suffix}");
        let subscription_name = format!("sub_progress_{suffix}");

        client
            .execute(
                &format!("create table {table_name}(id int primary key)"),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    "create subscription {subscription_name} from {table_name} with(retention = '1D')"
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    "declare progress_cur subscription cursor for {subscription_name} since now()"
                ),
                &[],
            )
            .await?;

        // Initialize the lazy `SINCE` cursor and leave it waiting for its first log-store epoch.
        let rows = client.query("fetch 1 from progress_cur", &[]).await?;
        test_eq!(rows.len(), 0);

        producer
            .execute(&format!("insert into {table_name} values (1)"), &[])
            .await?;
        producer.execute("flush", &[]).await?;
        producer
            .execute(
                &format!("insert into {table_name} select * from generate_series(2, 1000001)"),
                &[],
            )
            .await?;
        producer.execute("flush", &[]).await?;

        let cancel_token = client.cancel_token();
        let fetch_handle = tokio::spawn(async move {
            let result = client
                .query(
                    "fetch 2000000 from progress_cur with (timeout = '60s')",
                    &[],
                )
                .await;
            (client, result)
        });

        // The first epoch contributes row 1. The large following epoch keeps the next query active,
        // creating a real cancellation point after the first epoch boundary has been crossed.
        tokio::time::sleep(Duration::from_secs(1)).await;
        cancel_token.cancel_query(NoTls).await?;

        let (client, result) =
            tokio::time::timeout(Duration::from_secs(10), fetch_handle).await??;
        if result.is_ok() {
            return Err(anyhow!(
                "subscription FETCH with tentative epoch progress should be cancelled"
            ));
        }

        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            client.query("fetch 1 from progress_cur with (timeout = '0s')", &[]),
        )
        .await??;
        // Cancellation returned no partial result, but the same cursor must still expose one row
        // immediately through a nonblocking `FETCH`. Exact cache-position rollback is unit-tested.
        test_eq!(rows.len(), 1);

        client.execute("close progress_cur", &[]).await?;
        client
            .execute(&format!("drop subscription {subscription_name}"), &[])
            .await?;
        client
            .execute(&format!("drop table {table_name}"), &[])
            .await?;
        Ok(())
    }

    async fn query_cursor_fetch_cancel(&self, is_distributed: bool) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;
        client
            .execute(
                "declare query_cancel_cur cursor for select 1::int as id, pg_sleep(3)",
                &[],
            )
            .await?;

        let cancel_token = client.cancel_token();
        let fetch_handle = tokio::spawn(async move {
            let result = client.query("fetch 1 from query_cancel_cur", &[]).await;
            (client, result)
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_token.cancel_query(NoTls).await?;

        let (client, result) =
            tokio::time::timeout(Duration::from_secs(10), fetch_handle).await??;
        if result.is_ok() {
            return Err(anyhow!(
                "query cursor fetch should be cancelled by CancelRequest"
            ));
        }

        let rows = tokio::time::timeout(
            Duration::from_secs(10),
            client.query("fetch 1 from query_cancel_cur", &[]),
        )
        .await??;
        Self::assert_contiguous_ids(&rows, 1, 1)?;

        let rows = client.query("fetch 1 from query_cancel_cur", &[]).await?;
        test_eq!(rows.len(), 0);
        client.execute("close query_cancel_cur", &[]).await?;
        Ok(())
    }

    async fn cursor_queries_survive_statement_timeout(
        &self,
        is_distributed: bool,
    ) -> anyhow::Result<()> {
        const TOTAL_ROWS: usize = 100_000;
        const FETCH_SIZE: usize = 10_000;

        let client = self.create_client(is_distributed).await?;
        let suffix = if is_distributed { "dist" } else { "local" };
        let table_name = format!("cursor_timeout_t_{suffix}");
        let subscription_name = format!("cursor_timeout_sub_{suffix}");

        client
            .execute(
                &format!("create table {table_name}(id int primary key)"),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    "create subscription {subscription_name} from {table_name} with(retention = '1D')"
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!("insert into {table_name} select * from generate_series(1, {TOTAL_ROWS})"),
                &[],
            )
            .await?;
        client.execute("flush", &[]).await?;

        client
            .execute("set statement_timeout = '500ms'", &[])
            .await?;
        client
            .execute(
                &format!(
                    "declare query_timeout_cur cursor for select id from {table_name} order by id"
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    "declare subscription_timeout_cur subscription cursor for {subscription_name} full"
                ),
                &[],
            )
            .await?;

        for batch_index in 0..TOTAL_ROWS / FETCH_SIZE {
            let first_id = (batch_index * FETCH_SIZE + 1) as i32;

            let query_rows = client
                .query(&format!("fetch {FETCH_SIZE} from query_timeout_cur"), &[])
                .await?;
            Self::assert_contiguous_ids(&query_rows, first_id, FETCH_SIZE)?;

            let subscription_rows = client
                .query(
                    &format!("fetch {FETCH_SIZE} from subscription_timeout_cur"),
                    &[],
                )
                .await?;
            Self::assert_contiguous_ids(&subscription_rows, first_id, FETCH_SIZE)?;

            if batch_index + 1 < TOTAL_ROWS / FETCH_SIZE {
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
        }

        client.execute("set statement_timeout = 0", &[]).await?;
        test_eq!(
            client
                .query("fetch 1 from query_timeout_cur", &[])
                .await?
                .len(),
            0
        );
        test_eq!(
            client
                .query("fetch 1 from subscription_timeout_cur", &[])
                .await?
                .len(),
            0
        );

        client.execute("close query_timeout_cur", &[]).await?;
        client
            .execute("close subscription_timeout_cur", &[])
            .await?;
        client
            .execute(&format!("drop subscription {subscription_name}"), &[])
            .await?;
        client
            .execute(&format!("drop table {table_name}"), &[])
            .await?;
        Ok(())
    }

    async fn subquery_with_param(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        let res = client
            .query("select (select $1::SMALLINT)", &[&1024_i16])
            .await
            .unwrap();

        assert_eq!(res[0].get::<usize, i16>(0), 1024_i16);

        Ok(())
    }
}
