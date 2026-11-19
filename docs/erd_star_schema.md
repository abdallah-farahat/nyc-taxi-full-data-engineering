# Star schema (ERD)

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_TRIPS : "pickup_date_key"
    DIM_DATE ||--o{ FACT_TRIPS : "dropoff_date_key"
    DIM_TAXI_TYPE ||--o{ FACT_TRIPS : "taxi_type_key"
    DIM_LOCATION ||--o{ FACT_TRIPS : "pu_location_key"
    DIM_LOCATION ||--o{ FACT_TRIPS : "do_location_key"
    DIM_PAYMENT ||--o{ FACT_TRIPS : "payment_key"

    DIM_DATE ||--o{ AGG_TRIPS_DAILY : "pickup_date_key"
    DIM_TAXI_TYPE ||--o{ AGG_TRIPS_DAILY : "taxi_type_key"
    DIM_LOCATION ||--o{ AGG_TRIPS_DAILY : "pu_location_key"
    DIM_PAYMENT ||--o{ AGG_TRIPS_DAILY : "payment_key"

    DIM_DATE ||--o{ AGG_TRIPS_HOURLY : "pickup_date_key"
    DIM_TAXI_TYPE ||--o{ AGG_TRIPS_HOURLY : "taxi_type_key"

    DIM_DATE {
        int date_key PK "yyyymmdd"
        date full_date
        int calendar_year
        int month_number
        varchar year_month
        int day_of_week "1 = Monday"
        varchar day_name
        bit is_weekend
    }
    DIM_LOCATION {
        int location_key PK "TLC LocationID"
        varchar borough
        varchar zone
        varchar service_zone
    }
    DIM_PAYMENT {
        int payment_key PK "-1 = not recorded"
        varchar payment_name
        bit tip_is_recorded
    }
    DIM_TAXI_TYPE {
        int taxi_type_key PK
        varchar taxi_type "yellow or green"
        varchar description
    }
    FACT_TRIPS {
        binary trip_key UK "128-bit hash, unique by dbt test"
        int pickup_date_key FK
        int dropoff_date_key FK
        int taxi_type_key FK
        int pu_location_key FK
        int do_location_key FK
        int payment_key FK
        smallint pickup_hour
        datetime2 pickup_datetime
        datetime2 dropoff_datetime
        decimal trip_distance
        decimal trip_duration_min
        decimal fare_amount
        decimal tip_amount
        decimal total_amount
        bit is_flex_fare
        bit has_negative_amount
        varchar data_quality_flags
    }
    AGG_TRIPS_DAILY {
        int pickup_date_key PK
        int taxi_type_key PK
        int pu_location_key PK
        int payment_key PK
        bigint trip_count
        decimal fare_sum
        decimal total_sum
        decimal card_tip_sum
        decimal cbd_fee_sum
    }
    AGG_TRIPS_HOURLY {
        int pickup_date_key PK
        int pickup_hour PK
        int taxi_type_key PK
        bigint trip_count
        decimal total_sum
    }
```

Notes: the fact table has about 35 columns (only the main ones are drawn). It has no primary key on purpose
(a key index on 83 million rows is large and slow to load); uniqueness of `trip_key` is proven by a dbt test.