-- Q1: When is the demand highest? Trips by hour of day and day of week.
Select
    d.day_of_week,
    d.day_name,
    a.pickup_hour,
    t.taxi_type,
    sum(a.trip_count) As trips,
    count(distinct a.pickup_date_key) As days_in_data,
    cast(sum(a.trip_count) * 1.0 / count(distinct a.pickup_date_key) As Decimal(12, 2)) As avg_trips_per_day,
    cast(sum(a.total_sum) / nullif(sum(a.positive_trip_count), 0) As Decimal(10, 2)) As avg_total_per_trip
From {{ source('gold', 'agg_trips_hourly') }} a
Join {{ source('gold', 'dim_date') }} d On d.date_key = a.pickup_date_key
Join {{ source('gold', 'dim_taxi_type') }} t On t.taxi_type_key = a.taxi_type_key
Group By d.day_of_week, d.day_name, a.pickup_hour, t.taxi_type