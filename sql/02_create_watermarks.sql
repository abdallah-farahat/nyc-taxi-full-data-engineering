-- Phase 5: remembers how far Silver has read each Bronze table.
Use nyc_taxi_dw;
Go

Create Table audit.watermarks (
    source_table       Varchar(100) Not Null Primary Key,
    last_ingestion_us  Bigint       Not Null,   -- ingestion time in microseconds since 1970
    updated_at         Datetime2    Not Null Default sysutcdatetime()
);
Go

Select * From audit.watermarks;   -- empty at the start
Go