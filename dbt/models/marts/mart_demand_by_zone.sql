-- Q1: Where is the demand highest? Trips by pickup borough and zone.
Select
    l.borough,
    l.zone,
    t.taxi_type,
    sum(a.trip_count) As trips,
    cast(100.0 * sum(a.trip_count) / sum(sum(a.trip_count)) Over () As Decimal(6, 3)) As pct_of_all_trips
From {{ source('gold', 'agg_trips_daily') }} a
Join {{ source('gold', 'dim_location') }} l On l.location_key = a.pu_location_key
Join {{ source('gold', 'dim_taxi_type') }} t On t.taxi_type_key = a.taxi_type_key
Group By l.borough, l.zone, t.taxi_type