-- Phase 7: trip-level fact table of the star schema, stored as a clustered columnstore.
Use nyc_taxi_dw;
Go

-- Bulk loads of 83 million rows would make the log file huge under the full recovery model
Alter Database nyc_taxi_dw Set Recovery Simple;
Go

Create Table gold.fact_trips (
    trip_key              Binary(16)    Not Null,   -- first 128 bits of the SHA-256 trip_id
    pickup_date_key       Int           Not Null,
    pickup_hour           Smallint      Not Null,
    dropoff_date_key      Int           Not Null,
    taxi_type_key         Int           Not Null,
    pu_location_key       Int           Not Null,
    do_location_key       Int           Not Null,
    payment_key           Int           Not Null,
    vendor_id             Smallint      Null,
    ratecode_id           Smallint      Null,
    trip_type             Smallint      Null,
    request_source        Varchar(20)   Null,
    pickup_datetime       Datetime2(3)  Not Null,
    dropoff_datetime      Datetime2(3)  Not Null,
    pickup_month          Char(7)       Not Null,
    passenger_count       Smallint      Null,
    trip_distance         Decimal(10,2) Null,
    trip_duration_min     Decimal(10,2) Null,
    speed_mph             Decimal(10,2) Null,
    fare_amount           Decimal(14,2) Null,
    extra                 Decimal(14,2) Null,
    mta_tax               Decimal(14,2) Null,
    tip_amount            Decimal(14,2) Null,
    tolls_amount          Decimal(14,2) Null,
    improvement_surcharge Decimal(14,2) Null,
    congestion_surcharge  Decimal(14,2) Null,
    airport_fee           Decimal(14,2) Null,
    cbd_congestion_fee    Decimal(14,2) Null,
    total_amount          Decimal(14,2) Null,
    total_amount_diff     Decimal(14,2) Null,
    is_flex_fare          Bit           Null,
    is_store_and_forward  Bit           Null,
    is_tip_recorded       Bit           Null,
    has_negative_amount   Bit           Null,
    data_quality_flags    Varchar(600)  Null
);
Go

-- Columnstore: compressed storage, ideal for analytics over huge fact tables
Create Clustered Columnstore Index cci_fact_trips On gold.fact_trips;
Go

Select table_name From information_schema.tables Where table_schema = 'gold';   -- 7 tables
Go