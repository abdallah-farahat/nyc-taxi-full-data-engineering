-- Q3: How do revenue and average fare change over time for Yellow and Green,
-- and what did the new congestion fee (CBD) add?
-- The source sums are Float; here they become exact Decimal values.
Select
    d.year_month,
    t.taxi_type,
    sum(a.trip_count) As trips,
    sum(a.positive_trip_count) As positive_trips,
    cast(sum(a.total_sum) As Decimal(18, 2)) As total_revenue,
    cast(sum(a.fare_sum) As Decimal(18, 2)) As fare_revenue,
    cast(sum(a.fare_sum) / nullif(sum(a.positive_trip_count), 0) As Decimal(10, 2)) As avg_fare,
    cast(sum(a.distance_sum) As Decimal(18, 2)) As distance_miles,
    cast(sum(a.cbd_fee_sum) As Decimal(18, 2)) As cbd_fee_total,
    cast(sum(a.congestion_surcharge_sum) As Decimal(18, 2)) As congestion_surcharge_total
From {{ source('gold', 'agg_trips_daily') }} a
Join {{ source('gold', 'dim_date') }} d On d.date_key = a.pickup_date_key
Join {{ source('gold', 'dim_taxi_type') }} t On t.taxi_type_key = a.taxi_type_key
Group By d.year_month, t.taxi_type