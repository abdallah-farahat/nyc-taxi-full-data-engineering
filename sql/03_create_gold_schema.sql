-- Phase 7: serving layer tables in SQL Server (schema gold). Run once.
Use nyc_taxi_dw;
Go

Create Schema gold;
Go

Create Table gold.dim_date (
    date_key         Int         Not Null Primary Key,   -- 20250131
    full_date        Date        Not Null,
    calendar_year    Int         Not Null,
    quarter_number   Int         Not Null,
    month_number     Int         Not Null,
    month_name       Varchar(20) Not Null,
    year_month       Char(7)     Not Null,
    day_of_month     Int         Not Null,
    day_of_week      Int         Not Null,               -- 1 = Monday ... 7 = Sunday
    day_name         Varchar(20) Not Null,
    is_weekend       Bit         Not Null,
    week_of_year     Int         Not Null
);
Create Table gold.dim_location (
    location_key  Int          Not Null Primary Key,
    borough       Varchar(50)  Null,
    zone          Varchar(100) Null,
    service_zone  Varchar(50)  Null
);
Create Table gold.dim_payment (
    payment_key      Int         Not Null Primary Key,
    payment_name     Varchar(50) Not Null,
    tip_is_recorded  Bit         Not Null
);
Create Table gold.dim_taxi_type (
    taxi_type_key  Int         Not Null Primary Key,
    taxi_type      Varchar(10) Not Null,
    description    Varchar(50) Not Null
);

Create Table gold.agg_trips_daily (
    pickup_date_key           Int    Not Null,
    taxi_type_key             Int    Not Null,
    pu_location_key           Int    Not Null,
    payment_key               Int    Not Null,
    trip_count                Bigint Not Null,
    positive_trip_count       Bigint Not Null,
    flagged_trip_count        Bigint Not Null,
    distance_sum              Float  Null,
    duration_min_sum          Float  Null,
    fare_sum                  Float  Null,
    total_sum                 Float  Null,
    tolls_sum                 Float  Null,
    congestion_surcharge_sum  Float  Null,
    cbd_fee_sum               Float  Null,
    card_trip_count           Bigint Not Null,
    card_tip_sum              Float  Null,
    Constraint pk_agg_trips_daily
        Primary Key (pickup_date_key, taxi_type_key, pu_location_key, payment_key),
    Constraint fk_daily_date     Foreign Key (pickup_date_key) References gold.dim_date (date_key),
    Constraint fk_daily_taxi     Foreign Key (taxi_type_key)   References gold.dim_taxi_type (taxi_type_key),
    Constraint fk_daily_location Foreign Key (pu_location_key) References gold.dim_location (location_key),
    Constraint fk_daily_payment  Foreign Key (payment_key)     References gold.dim_payment (payment_key)
);
Create Table gold.agg_trips_hourly (
    pickup_date_key      Int    Not Null,
    pickup_hour          Int    Not Null,
    taxi_type_key        Int    Not Null,
    trip_count           Bigint Not Null,
    positive_trip_count  Bigint Not Null,
    total_sum            Float  Null,
    distance_sum         Float  Null,
    duration_min_sum     Float  Null,
    Constraint pk_agg_trips_hourly Primary Key (pickup_date_key, pickup_hour, taxi_type_key),
    Constraint fk_hourly_date Foreign Key (pickup_date_key) References gold.dim_date (date_key),
    Constraint fk_hourly_taxi Foreign Key (taxi_type_key)   References gold.dim_taxi_type (taxi_type_key)
);
Go

Select table_name From information_schema.tables Where table_schema = 'gold';
Go