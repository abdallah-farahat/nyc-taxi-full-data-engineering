Select * From {{ source('gold', 'agg_trips_daily') }}
Where trip_count <= 0
   Or positive_trip_count < 0 Or positive_trip_count > trip_count
   Or flagged_trip_count > trip_count
   Or card_trip_count > positive_trip_count