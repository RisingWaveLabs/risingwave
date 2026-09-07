-- PG
DROP TABLE IF EXISTS shipments;
CREATE TABLE shipments (
  shipment_id SERIAL NOT NULL PRIMARY KEY,
  order_id SERIAL NOT NULL,
  origin VARCHAR(255) NOT NULL,
  destination VARCHAR(255) NOT NULL,
  is_arrived BOOLEAN NOT NULL
);
ALTER SEQUENCE public.shipments_shipment_id_seq RESTART WITH 1001;
ALTER TABLE public.shipments REPLICA IDENTITY FULL;
INSERT INTO shipments
VALUES (default,10001,'Beijing','Shanghai',false),
       (default,10002,'Hangzhou','Shanghai',false),
       (default,10003,'Shanghai','Hangzhou',false);


CREATE TABLE person (
    "id" int,
    "name" varchar(64),
    "email_address" varchar(200),
    "credit_card" varchar(200),
    "city" varchar(200),
    PRIMARY KEY ("id")
);

ALTER TABLE
    public.person REPLICA IDENTITY FULL;

INSERT INTO person VALUES (1000, 'vicky noris', 'yplkvgz@qbxfg.com', '7878 5821 1864 2539', 'cheyenne');
INSERT INTO person VALUES (1001, 'peter white', 'myckhsp@xpmpe.com', '1781 2313 8157 6974', 'boise');
INSERT INTO person VALUES (1002, 'sarah spencer', 'wipvdbm@dkaap.com', '3453 4987 9481 6270', 'los angeles');

create schema abs;
create table abs.t1 ("V1" int primary key, v2 double precision, v3 varchar, v4 numeric);
create publication my_publicaton for table abs.t1 ("V1", v3);
insert into abs.t1 values (1, 1.1, 'aaa', '5431.1234');

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

DROP TABLE IF EXISTS order_ledger_entries_composite;
DROP TYPE IF EXISTS outer_type;
DROP TYPE IF EXISTS inner_type;
DROP TYPE IF EXISTS money_type;

CREATE TYPE money_type AS (
    currency varchar,
    amount decimal
);

CREATE TYPE inner_type AS (
    n int,
    flag boolean,
    note text
);

CREATE TYPE outer_type AS (
    name text,
    payload inner_type
);

CREATE TABLE order_ledger_entries_composite (
    id int PRIMARY KEY,
    note_varchar varchar,
    total money_type,
    listed_price money_type,
    listed_discount money_type NOT NULL,
    complex_col outer_type,
    prices money_type[]
);
ALTER TABLE order_ledger_entries_composite ALTER COLUMN prices SET STORAGE EXTERNAL;

INSERT INTO order_ledger_entries_composite VALUES
    (
        1,
        'plain_text_1',
        ROW('USD', 12.34)::money_type,
        ROW('USD', 20.00)::money_type,
        ROW('USD', 7.66)::money_type,
        ROW('foo', ROW(1, false, 'hello')::inner_type)::outer_type,
        ARRAY[ROW('USD', 1.00)::money_type, ROW('EUR', 2.00)::money_type]::money_type[]
    ),
    (
        2,
        'KEEp',
        ROW('EUR', 88.80)::money_type,
        ROW('EUR', 99.90)::money_type,
        ROW('EUR', 11.10)::money_type,
        ROW('bar', ROW(2, true, 'world')::inner_type)::outer_type,
        NULL
    );

CREATE TABLE IF NOT EXISTS postgres_all_types(
     c_boolean boolean,
     c_smallint smallint,
     c_integer integer,
     c_bigint bigint,
     c_decimal decimal,
     c_real real,
     c_double_precision double precision,
     c_varchar varchar,
     c_bytea bytea,
     c_date date,
     c_time time,
     c_timestamp timestamp,
     c_timestamptz timestamptz,
     c_interval interval,
     c_jsonb jsonb,
     c_uuid uuid,
     c_enum mood,
     c_boolean_array boolean[],
     c_smallint_array smallint[],
     c_integer_array integer[],
     c_bigint_array bigint[],
     c_decimal_array decimal[],
     c_real_array real[],
     c_double_precision_array double precision[],
     c_varchar_array varchar[],
     c_bytea_array bytea[],
     c_date_array date[],
     c_time_array time[],
     c_timestamp_array timestamp[],
     c_timestamptz_array timestamptz[],
     c_interval_array interval[],
     c_jsonb_array jsonb[],
     c_uuid_array uuid[],
     c_enum_array mood[],
     PRIMARY KEY (c_boolean,c_bigint,c_date)
);
INSERT INTO postgres_all_types VALUES ( False, 0, 0, 0, 0, 0, 0, '', '\x00', '0001-01-01', '00:00:00', '2001-01-01 00:00:00'::timestamp, '2001-01-01 00:00:00-8'::timestamptz, interval '0 second', '{}', null, 'sad', array[]::boolean[], array[]::smallint[], array[]::integer[], array[]::bigint[], array[]::decimal[], array[]::real[], array[]::double precision[], array[]::varchar[], array[]::bytea[], array[]::date[], array[]::time[], array[]::timestamp[], array[]::timestamptz[], array[]::interval[], array[]::jsonb[], array[]::uuid[], array[]::mood[]);
INSERT INTO postgres_all_types VALUES ( False, -32767, -2147483647, -9223372036854775807, -10.0, -9999.999999, -10000.0, 'd', '\x00', '0001-01-01', '00:00:00', '2001-01-01 00:00:00'::timestamp, '2001-01-01 00:00:00-8'::timestamptz, interval '0 second', '{}', 'bb488f9b-330d-4012-b849-12adeb49e57e', 'happy', array[False::boolean]::boolean[], array[-32767::smallint]::smallint[], array[-2147483647::integer]::integer[], array[-9223372036854775807::bigint]::bigint[], array[-10.0::decimal]::decimal[], array[-9999.999999::real]::real[], array[-10000.0::double precision]::double precision[], array[''::varchar]::varchar[], array['\x00'::bytea]::bytea[], array['0001-01-01'::date]::date[], array['00:00:00'::time]::time[], array['2001-01-01 00:00:00'::timestamp::timestamp]::timestamp[], array['2001-01-01 00:00:00-8'::timestamptz::timestamptz]::timestamptz[], array[interval '0 second'::interval]::interval[], array['{}'::jsonb]::jsonb[], '{bb488f9b-330d-4012-b849-12adeb49e57e}', '{happy,ok,NULL}');
INSERT INTO postgres_all_types VALUES ( False, 1, 123, 1234567890, 123.45, 123.45, 123.456, 'example', '\xDEADBEEF', '0024-01-01', '12:34:56', '2024-05-19 12:34:56', '2024-05-19 12:34:56+00', INTERVAL '1 day', '{"key": "value"}', '123e4567-e89b-12d3-a456-426614174000', 'happy', ARRAY[NULL, TRUE]::boolean[], ARRAY[NULL, 1::smallint], ARRAY[NULL, 123], ARRAY[NULL, 1234567890], ARRAY[NULL, 123.45::numeric], ARRAY[NULL, 123.45::real], ARRAY[NULL, 123.456], ARRAY[NULL, 'example'], ARRAY[NULL, '\xDEADBEEF'::bytea], ARRAY[NULL, '2024-05-19'::date], ARRAY[NULL, '12:34:56'::time], ARRAY[NULL, '2024-05-19 12:34:56'::timestamp], ARRAY[NULL, '2024-05-19 12:34:56+00'::timestamptz], ARRAY[NULL, INTERVAL '1 day'], ARRAY[NULL, '{"key": "value"}'::jsonb], ARRAY[NULL, '123e4567-e89b-12d3-a456-426614174000'::uuid], ARRAY[NULL, 'happy'::mood]);
INSERT INTO postgres_all_types VALUES ( False, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '0024-05-19', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

create table numeric_table(id int PRIMARY KEY, num numeric);
insert into numeric_table values(1, 3.14);
--- 2^255 - 1
insert into numeric_table values(2, 57896044618658097711785492504343953926634992332820282019728792003956564819967);
--- 2^255
insert into numeric_table values(3, 57896044618658097711785492504343953926634992332820282019728792003956564819968);
--- 2^256
insert into numeric_table values(4, 115792089237316195423570985008687907853269984665640564039457584007913129639936);
insert into numeric_table values(5, 115792089237316195423570985008687907853269984665640564039457584007913129639936.555555);
insert into numeric_table values(6, 'NaN'::numeric);
insert into numeric_table values(7, 'Infinity'::numeric);

create table numeric_list(id int primary key, num numeric[]);
insert into numeric_list values(1, '{3.14, 6, 57896044618658097711785492504343953926634992332820282019728792003956564819967, 57896044618658097711785492504343953926634992332820282019728792003956564819968, 115792089237316195423570985008687907853269984665640564039457584007913129639936.555555}');
insert into numeric_list values(2, '{nan, infinity, -infinity}');

CREATE TABLE enum_table (
    id int PRIMARY KEY,
    current_mood mood
);
INSERT INTO enum_table VALUES (1, 'happy');

CREATE TABLE list_with_null(id int primary key, my_int int[], my_num numeric[], my_num_1 numeric[], my_num_2 numeric[], my_mood mood[], my_uuid uuid[], my_bytea bytea[]);
INSERT INTO list_with_null VALUES (1, '{1,2,NULL}', '{1.1,inf,NULL}', '{1.1,inf,NULL}', '{1.1,inf,NULL}', '{happy,ok,NULL}', '{bb488f9b-330d-4012-b849-12adeb49e57e,bb488f9b-330d-4012-b849-12adeb49e57f, NULL}', '{\\x00,\\x01,NULL}');
INSERT INTO list_with_null VALUES (2, '{NULL,3,4}', '{2.2,0,NULL}' , '{2.2,0,NULL}', '{2.2,0,NULL}', '{happy,ok,sad}', '{2de296df-eda7-4202-a81f-1036100ef4f6,2977afbc-0b12-459c-a36f-f623fc9e9840}', '{\\x00,\\x01,\\x02}');
INSERT INTO list_with_null VALUES (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE "Orders" (
    id int PRIMARY KEY,
    name varchar
);
INSERT INTO "Orders" VALUES (1, 'happy');


CREATE TABLE IF NOT EXISTS partitioned_timestamp_table(
     c_int int,
     c_boolean boolean,
     c_timestamp timestamp,
     PRIMARY KEY (c_int, c_timestamp)
) PARTITION BY RANGE (c_timestamp);

CREATE TABLE partitioned_timestamp_table_2023 PARTITION OF partitioned_timestamp_table
    FOR VALUES FROM ('2023-01-01') TO ('2023-12-31') PARTITION BY RANGE (c_timestamp);

CREATE TABLE partitioned_timestamp_table_2024 PARTITION OF partitioned_timestamp_table
    FOR VALUES FROM ('2024-01-01') TO ('2024-12-31');

CREATE TABLE partitioned_timestamp_table_2025 PARTITION OF partitioned_timestamp_table
    FOR VALUES FROM ('2025-01-01') TO ('2025-12-31');

CREATE TABLE partitioned_timestamp_table_2023_h1 PARTITION OF partitioned_timestamp_table_2023
    FOR VALUES FROM ('2023-01-01') TO ('2023-06-30');

CREATE TABLE partitioned_timestamp_table_2023_h2 PARTITION OF partitioned_timestamp_table_2023
    FOR VALUES FROM ('2023-07-01') TO ('2024-12-31');

INSERT INTO partitioned_timestamp_table (c_int, c_boolean, c_timestamp) VALUES
(1, false, '2023-02-01 10:30:00'),
(2, false, '2023-05-15 11:45:00'),
(3, false, '2023-11-03 12:15:00'),
(4, false, '2024-01-04 13:00:00'),
(5, false, '2024-03-05 09:30:00'),
(6, false, '2024-06-06 14:20:00'),
(7, false, '2024-09-07 16:45:00'),
(8, false, '2025-01-08 18:30:00'),
(9, false, '2025-07-09 07:10:00');

-- Here we create this publication without `WITH ( publish_via_partition_root = true )` only for tests. Normally, it should be added.
create publication rw_publication_pubviaroot_false for TABLE partitioned_timestamp_table;


CREATE TABLE test_pg_default_value (
    "id" int,
    "name" varchar(64),
    "city" varchar(200) default 'Shanghai',
    PRIMARY KEY ("id")
);

INSERT INTO test_pg_default_value(id, name) VALUES (1, 'bugen');

CREATE TABLE json_table(
    id INT,
    c_json JSON,
    c_jsonb JSONB,
    c_json_array JSON[],
    c_jsonb_array JSONB[],
    PRIMARY KEY (id)
);

INSERT INTO
  json_table
VALUES
  (1, '{"id": 1, "name": "alice"}', '{"id": 1, "name": "alice"}', ARRAY['{"id": 1, "name": "alice"}'::json], ARRAY['{"id": 1, "name": "alice"}'::jsonb]),
  (2, '{}', '{}', ARRAY['{}'::json], ARRAY['{}'::jsonb]),
  (3, '{}', '{}', array[]::json[], array[]::jsonb[]),
  (4, NULL, NULL, NULL, NULL);
