SET NOCOUNT ON;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  order_date BIGINT,
  customer_name NVARCHAR(200),
  price DECIMAL,
  product_id INT,
  order_status SMALLINT
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'orders',
  @role_name = NULL;


INSERT INTO
  orders (
    order_id,
    order_date,
    customer_name,
    price,
    product_id,
    order_status
  )
VALUES
  (1, 1558430840000, 'Bob', 10.50, 1, 1),
  (2, 1558430840001, 'Alice', 20.50, 2, 1),
  (3, 1558430840002, 'Alice', 18.50, 2, 1);

CREATE TABLE single_type (
  id INT PRIMARY KEY,
  c_time time
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'single_type',
  @role_name = NULL;

INSERT INTO single_type VALUES (3, '23:59:59.999');


CREATE TABLE sqlserver_all_data_types (
  id INT PRIMARY KEY,
  c_bit bit,
  c_tinyint tinyint,
  c_smallint smallint,
  c_int int,
  c_bigint bigint,
  c_decimal DECIMAL(28),
  c_real real,
  c_float float,
  c_char char(4),
  c_varchar varchar(4),
  c_nvarchar nvarchar(4),
  c_ntext ntext,
  c_binary binary(4),
  c_varbinary varbinary(4),
  c_uniqueidentifier uniqueidentifier,
  c_date date,
  c_time time,
  c_datetime2 datetime2,
  c_datetimeoffset datetimeoffset,
  c_xml xml,
  c_money money
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'sqlserver_all_data_types',
  @role_name = NULL;

INSERT INTO sqlserver_all_data_types VALUES (1, 'False', 0, 0, 0, 0, 0, 0, 0, '', '', N'中', N'中', 0xff, NULL, NULL, '2001-01-01', '00:00:00', '2001-01-01 00:00:00', '2001-01-01 00:00:00', '<Person><Name>John Doe</Name><Age>30</Age></Person>', 100.510);

INSERT INTO sqlserver_all_data_types VALUES (2, 'True', 255, -32768, -2147483648, -9223372036854775808, -10.0, -9999.999999, -10000.0, 'aa', 'aa', N'🌹', N'🌹', NULL, 0xff, '6f9619ff-8b86-d011-b42d-00c04fc964ff', '1990-01-01', '13:59:59.123', '2000-01-01 11:00:00.123', '2026-06-23 00:01:05.0900948 -07:00', '<Person> <Name>Jane Doe</Name> <Age>28</Age> </Person>', 100.5);

INSERT INTO sqlserver_all_data_types VALUES (3, 'True', 127, 32767, 2147483647, 9223372036854775807, -10.0, 9999.999999, 10000.0, 'zzzz', 'zzzz', N'🌹👍', N'🌹👍', 0xffffffff, 0xffffffff, '6F9619FF-8B86-D011-B42D-00C04FC964FF', '2999-12-31', '23:59:59.999', '2099-12-31 23:59:59.999', '2999-12-31 23:59:59.999', '<Name>Jane Doe</Name>', 100.0)

-- Table without enabling CDC
CREATE TABLE orders_without_cdc (
  order_id INT PRIMARY KEY,
  order_date BIGINT,
  customer_name NVARCHAR(200),
  price DECIMAL,
  product_id INT,
  order_status SMALLINT
);


CREATE TABLE test_pk_uuid (
  id UNIQUEIDENTIFIER PRIMARY KEY,
  NAME NVARCHAR(50)
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'test_pk_uuid',
  @role_name = NULL;

CREATE TABLE test_pk_binary (
  id BINARY(50) PRIMARY KEY,
  NAME NVARCHAR(50)
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'test_pk_binary',
  @role_name = NULL;

DECLARE @i INT = 1;

WHILE @i <= 2000
BEGIN
    INSERT INTO test_pk_uuid (id, NAME)
    VALUES (NEWID(), CONCAT('TEST_NAME', @i));

    INSERT INTO test_pk_binary (id, NAME)
    VALUES (HASHBYTES('SHA2_256', CONCAT('TEST_NAME', @i)), CONCAT('TEST_NAME', @i));

    SET @i = @i + 1;
END
