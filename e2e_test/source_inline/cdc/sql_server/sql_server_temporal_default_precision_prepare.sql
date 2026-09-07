-- SQL Server CDC temporal type test preparation.
-- This file prepares an empty table so the CDC source can be created before inserts.

USE sqlserver_temporal_default_precision_db;
GO

EXEC sys.sp_cdc_enable_db;
GO

CREATE TABLE temporal_test (
    id INT PRIMARY KEY,
    c_date DATE,
    c_time TIME(3),
    c_time_1 TIME(1),
    c_time_5 TIME(5),
    c_time_7 TIME(7),
    c_datetime DATETIME,
    c_smalldatetime SMALLDATETIME,
    c_datetime2 DATETIME2(3),
    c_datetime2_5 DATETIME2(5),
    c_datetime2_7 DATETIME2(7),
    c_datetimeoffset DATETIMEOFFSET(3)
);
GO

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'temporal_test', @role_name = NULL;
GO
